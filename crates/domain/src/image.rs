//! The decoded pixel buffer handed from `apollo-media` (producer) to
//! `apollo-inference` (consumer). Lives here so neither sibling crate depends on
//! the other.

/// A decoded image in packed RGB8: row-major, 3 bytes (R, G, B) per pixel, so
/// `data.len() == width * height * 3`.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedImage {
    pub width: u32,
    pub height: u32,
    pub data: Vec<u8>,
}

impl DecodedImage {
    /// Number of pixels (`width * height`).
    pub fn pixel_count(&self) -> usize {
        self.width as usize * self.height as usize
    }
}
