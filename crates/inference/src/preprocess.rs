//! Pixel → tensor preparation: resize, scale to [0,1], per-channel normalize.

use candle_core::{DType, Device, Tensor};

use apollo_domain::DecodedImage;

use crate::error::InferenceError;
use crate::loader::Hub;

/// How a model wants its pixels: square resize target plus per-channel mean/std.
#[derive(Debug, Clone)]
pub struct Normalization {
    pub size: usize,
    pub mean: [f32; 3],
    pub std: [f32; 3],
}

impl Normalization {
    /// The centered default many ViT checkpoints use: mean/std of 0.5 → [-1, 1].
    pub fn centered(size: usize) -> Self {
        Self {
            size,
            mean: [0.5; 3],
            std: [0.5; 3],
        }
    }
}

/// Resize each image to `size`×`size`, convert to CHW f32 in [0,1], normalize, and
/// stack into a batch tensor `(B, 3, size, size)`.
pub(crate) fn preprocess(
    images: &[DecodedImage],
    norm: &Normalization,
    device: &Device,
) -> Result<Tensor, InferenceError> {
    let mean = Tensor::from_vec(norm.mean.to_vec(), (3, 1, 1), device)?;
    let std = Tensor::from_vec(norm.std.to_vec(), (3, 1, 1), device)?;

    let mut tensors = Vec::with_capacity(images.len());
    for img in images {
        let resized = resize_rgb(img, norm.size as u32)?;
        let t = Tensor::from_vec(resized, (norm.size, norm.size, 3), device)?
            .to_dtype(DType::F32)?
            .permute((2, 0, 1))? // HWC -> CHW
            .affine(1.0 / 255.0, 0.0)?; // scale to [0, 1]
        let t = t.broadcast_sub(&mean)?.broadcast_div(&std)?;
        tensors.push(t);
    }
    Ok(Tensor::stack(&tensors, 0)?)
}

fn resize_rgb(img: &DecodedImage, size: u32) -> Result<Vec<u8>, InferenceError> {
    let buf =
        image::RgbImage::from_raw(img.width, img.height, img.data.clone()).ok_or_else(|| {
            InferenceError::Preprocess("image buffer length does not match dimensions".into())
        })?;
    let resized = image::imageops::resize(&buf, size, size, image::imageops::FilterType::Triangle);
    Ok(resized.into_raw())
}

/// Build a [`Normalization`] for a repo: read `preprocessor_config.json` if present
/// (`image_mean` / `image_std` / `size`), otherwise the centered default at
/// `fallback_size` (the model's `image_size`). Any failure falls back.
pub(crate) fn normalization_for(hub: &Hub, fallback_size: usize) -> Normalization {
    let parsed = hub
        .file("preprocessor_config.json")
        .ok()
        .and_then(|p| std::fs::read(p).ok())
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok());

    let Some(v) = parsed else {
        return Normalization::centered(fallback_size);
    };
    Normalization {
        size: read_size(&v).unwrap_or(fallback_size),
        mean: read_triple(&v, "image_mean").unwrap_or([0.5; 3]),
        std: read_triple(&v, "image_std").unwrap_or([0.5; 3]),
    }
}

fn read_triple(v: &serde_json::Value, key: &str) -> Option<[f32; 3]> {
    let arr = v.get(key)?.as_array()?;
    if arr.len() != 3 {
        return None;
    }
    let mut out = [0f32; 3];
    for (i, x) in arr.iter().enumerate() {
        out[i] = x.as_f64()? as f32;
    }
    Some(out)
}

fn read_size(v: &serde_json::Value) -> Option<usize> {
    let size = v.get("size")?;
    if let Some(n) = size.as_u64() {
        return Some(n as usize);
    }
    if let Some(h) = size.get("height").and_then(|x| x.as_u64()) {
        return Some(h as usize);
    }
    if let Some(s) = size.get("shortest_edge").and_then(|x| x.as_u64()) {
        return Some(s as usize);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preprocess_produces_batched_chw() {
        let img = DecodedImage {
            width: 2,
            height: 2,
            data: vec![0u8; 2 * 2 * 3],
        };
        let norm = Normalization::centered(4);
        let batch = preprocess(&[img.clone(), img], &norm, &Device::Cpu).unwrap();
        assert_eq!(batch.dims(), &[2, 3, 4, 4]);
    }
}
