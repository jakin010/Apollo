//! Image decoding (any format the `image` crate supports) into RGB8.

use apollo_domain::DecodedImage;

use crate::error::MediaError;

/// Decode encoded image bytes into a packed RGB8 [`DecodedImage`].
pub fn decode_image(bytes: &[u8]) -> Result<DecodedImage, MediaError> {
    let img = image::load_from_memory(bytes).map_err(|e| MediaError::Decode(e.to_string()))?;
    let rgb = img.to_rgb8();
    let (width, height) = rgb.dimensions();
    Ok(DecodedImage {
        width,
        height,
        data: rgb.into_raw(),
    })
}
