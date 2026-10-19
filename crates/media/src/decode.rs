//! Image decoding (any format the `image` crate supports) into RGB8, upright
//! (EXIF orientation applied), with a pixel-count cap to defend against
//! decompression bombs.

use std::io::Cursor;

use apollo_domain::DecodedImage;

use crate::error::MediaError;

/// Decode encoded image bytes into a packed RGB8 [`DecodedImage`], rotated
/// or flipped as its EXIF orientation says (the way viewers show it), so a
/// phone photo stored sideways is classified and hashed upright.
///
/// `max_pixels` bounds the decoded resolution (`width * height`). A tiny, highly
/// compressible image can otherwise declare enormous dimensions and decode to
/// gigabytes of pixels; the dimensions are read from the header first and the
/// image is rejected before the pixel buffer is ever allocated. `None` disables
/// the cap.
pub fn decode_image(bytes: &[u8], max_pixels: Option<u64>) -> Result<DecodedImage, MediaError> {
    decode(bytes, max_pixels, true)
}

/// [`decode_image`] without applying the EXIF orientation: the pixels as
/// stored, as most image tools (Meta's PDQ hasher among them) read them.
pub fn decode_image_as_stored(
    bytes: &[u8],
    max_pixels: Option<u64>,
) -> Result<DecodedImage, MediaError> {
    decode(bytes, max_pixels, false)
}

fn decode(
    bytes: &[u8],
    max_pixels: Option<u64>,
    upright: bool,
) -> Result<DecodedImage, MediaError> {
    if let Some(max_px) = max_pixels {
        // Read just the header (no full decode, no large allocation) and reject an
        // oversized image up front. This is the guard against decompression bombs.
        let (w, h) = image::ImageReader::new(Cursor::new(bytes))
            .with_guessed_format()
            .map_err(|e| MediaError::Decode(format!("reading image header: {e}")))?
            .into_dimensions()
            .map_err(|e| MediaError::Decode(e.to_string()))?;
        let pixels = u64::from(w) * u64::from(h);
        if pixels > max_px {
            return Err(MediaError::Decode(format!(
                "image {w}x{h} ({pixels} pixels) exceeds the {max_px}-pixel decode limit"
            )));
        }
    }

    use image::ImageDecoder;
    let decode_err = |e: image::ImageError| MediaError::Decode(e.to_string());
    let mut decoder = image::ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|e| MediaError::Decode(format!("reading image header: {e}")))?
        .into_decoder()
        .map_err(decode_err)?;
    // A missing or unreadable orientation means "as stored".
    let orientation = decoder
        .orientation()
        .unwrap_or(image::metadata::Orientation::NoTransforms);
    let mut img = image::DynamicImage::from_decoder(decoder).map_err(decode_err)?;
    if upright {
        img.apply_orientation(orientation);
    }
    let rgb = img.to_rgb8();
    let (width, height) = rgb.dimensions();
    Ok(DecodedImage {
        width,
        height,
        data: rgb.into_raw(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 4×2 JPEG (left half black, right half white) carrying an EXIF
    /// orientation tag.
    fn jpeg_with_orientation(orientation: u16) -> Vec<u8> {
        let mut img = image::RgbImage::new(4, 2);
        for (x, _, px) in img.enumerate_pixels_mut() {
            *px = if x < 2 {
                image::Rgb([0, 0, 0])
            } else {
                image::Rgb([255, 255, 255])
            };
        }
        let mut jpeg = Vec::new();
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg, 100)
            .encode_image(&img)
            .unwrap();
        // APP1 "Exif": a little-endian TIFF header with one IFD entry,
        // Orientation (0x0112, SHORT, 1 value).
        let mut tiff = vec![b'I', b'I', 42, 0, 8, 0, 0, 0, 1, 0];
        tiff.extend([0x12, 0x01, 3, 0, 1, 0, 0, 0]);
        tiff.extend(orientation.to_le_bytes());
        tiff.extend([0, 0, 0, 0, 0, 0]);
        let mut app1 = b"Exif\0\0".to_vec();
        app1.extend(tiff);
        let len = (app1.len() + 2) as u16;
        let mut out = jpeg[..2].to_vec();
        out.extend([0xFF, 0xE1]);
        out.extend(len.to_be_bytes());
        out.extend(app1);
        out.extend(&jpeg[2..]);
        out
    }

    #[test]
    fn exif_orientation_is_applied() {
        let plain = decode_image(&jpeg_with_orientation(1), None).unwrap();
        assert_eq!((plain.width, plain.height), (4, 2));
        // 6 = rotate 90° clockwise: 4×2 becomes 2×4, and the left (black)
        // half ends up on top.
        let rotated = decode_image(&jpeg_with_orientation(6), None).unwrap();
        assert_eq!((rotated.width, rotated.height), (2, 4));
        let stored = decode_image_as_stored(&jpeg_with_orientation(6), None).unwrap();
        assert_eq!((stored.width, stored.height), (4, 2));
        assert!(rotated.data[0] < 64, "top-left is black");
        let bottom_left = ((3 * 2) * 3) as usize;
        assert!(rotated.data[bottom_left] > 192, "bottom-left is white");
    }

    #[test]
    fn the_pixel_cap_is_checked_before_decoding() {
        let err = decode_image(&jpeg_with_orientation(1), Some(7)).unwrap_err();
        assert!(err.to_string().contains("exceeds the 7-pixel"), "{err}");
    }
}
