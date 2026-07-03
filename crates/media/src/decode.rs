//! Image decoding (any format the `image` crate supports) into RGB8, with a
//! pixel-count cap to defend against decompression bombs.

use std::io::Cursor;

use apollo_domain::DecodedImage;

use crate::error::MediaError;

/// Decode encoded image bytes into a packed RGB8 [`DecodedImage`].
///
/// `max_pixels` bounds the decoded resolution (`width * height`). A tiny, highly
/// compressible image can otherwise declare enormous dimensions and decode to
/// gigabytes of pixels; the dimensions are read from the header first and the
/// image is rejected before the pixel buffer is ever allocated. `None` disables
/// the cap.
pub fn decode_image(bytes: &[u8], max_pixels: Option<u64>) -> Result<DecodedImage, MediaError> {
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

    let img = image::load_from_memory(bytes).map_err(|e| MediaError::Decode(e.to_string()))?;
    let rgb = img.to_rgb8();
    let (width, height) = rgb.dimensions();
    Ok(DecodedImage {
        width,
        height,
        data: rgb.into_raw(),
    })
}
