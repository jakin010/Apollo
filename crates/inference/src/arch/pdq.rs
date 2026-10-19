//! PDQ perceptual hashing (Meta's 256-bit image hash) and a block-list
//! classifier built on it.
//!
//! The hash follows Meta's reference implementation (ThreatExchange
//! `pdq/cpp`) down to the order of float operations, and matches it bit for
//! bit (see the `hashes_match_metas_reference` test):
//! 0. **Resize:** an image larger than 512 px on a side becomes 512×512 by
//!    nearest neighbour, as Meta's file hasher does.
//! 1. **Luminance:** RGB to luminance (0.299 R + 0.587 G + 0.114 B).
//! 2. **Downsample:** two passes of a Jarosz box filter (window `⌈size / 128⌉`
//!    along each axis), then decimation to 64×64.
//! 3. **DCT:** a 16×16 DCT of the 64×64 buffer, without the DC row and column.
//! 4. **Bits:** one bit per coefficient, set when it is above the median.
//!
//! A quality score (0–100) measures how much detail the 64×64 image has; flat
//! images hash unreliably. Hashes are written as 64 hex digits, most
//! significant first, like other PDQ tools, so lists can be exchanged. Two
//! hashes of the same picture (re-encoded, resized, lightly edited) differ in
//! few bits; unrelated pictures differ in about half (128). The 8 dihedral
//! hashes (rotations and mirrors) come from the same DCT.
//!
//! As a model (`architecture = "pdq"`), it compares each image's hash with a
//! list of known hashes, each tagged with a category, and predicts the
//! categories with a hash within `max_distance` bits.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use apollo_config::ModelConfig;
use apollo_domain::{Classification, DecodedImage, Label, Prediction};

use crate::ImageClassifier;
use crate::error::InferenceError;

/// A 256-bit PDQ hash. Bit `16 * i + j` is DCT coefficient `(i, j)`; word 0
/// holds bits 0–63.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct PdqHash(pub [u64; 4]);

impl PdqHash {
    /// Bits that differ (0 to 256).
    pub fn distance(&self, other: &PdqHash) -> u32 {
        self.0
            .iter()
            .zip(&other.0)
            .map(|(a, b)| (a ^ b).count_ones())
            .sum()
    }

    fn set(&mut self, bit: usize) {
        self.0[bit / 64] |= 1 << (bit % 64);
    }
}

impl fmt::Display for PdqHash {
    /// 64 lowercase hex digits, most significant first.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for word in self.0.iter().rev() {
            write!(f, "{word:016x}")?;
        }
        Ok(())
    }
}

impl FromStr for PdqHash {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        if s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("'{s}' is not a PDQ hash (64 hex digits)"));
        }
        let mut words = [0u64; 4];
        for (i, word) in words.iter_mut().rev().enumerate() {
            *word = u64::from_str_radix(&s[i * 16..(i + 1) * 16], 16).map_err(|e| e.to_string())?;
        }
        Ok(PdqHash(words))
    }
}

/// Images with a side shorter than this get an empty hash of quality 0, as
/// in the reference.
const MIN_SIDE: usize = 5;
/// Images with a side longer than this are first resized to `RESIZE`×`RESIZE`
/// by nearest neighbour, as Meta's file hasher does (with CImg), so a hash
/// matches what Meta's tools compute for the same image.
const RESIZE: usize = 512;

/// The PDQ hash of an image and its quality (0–100; below 50 is unreliable).
pub fn pdq_hash(image: &DecodedImage) -> (PdqHash, u32) {
    match pdq_dct(image) {
        Some((dct, quality)) => (bits(&dct), quality),
        None => (PdqHash::default(), 0),
    }
}

/// The image's 8 dihedral hashes — the hashes of its rotations by 90°, 180°
/// and 270° and its mirror images, derived from one DCT — and its quality.
/// In the reference's order: original, rotate 90, rotate 180, rotate 270,
/// flip x, flip y, flip about the diagonal, flip about the anti-diagonal.
pub fn pdq_dihedral_hashes(image: &DecodedImage) -> ([PdqHash; 8], u32) {
    let Some((dct, quality)) = pdq_dct(image) else {
        return ([PdqHash::default(); 8], 0);
    };
    let variant = |f: fn(usize, usize) -> (usize, usize, bool)| {
        let mut out = [0f32; 256];
        for i in 0..16 {
            for j in 0..16 {
                let (oi, oj, negate) = f(i, j);
                let v = dct[i * 16 + j];
                out[oi * 16 + oj] = if negate { -v } else { v };
            }
        }
        bits(&out)
    };
    fn odd(n: usize) -> bool {
        n & 1 == 1
    }
    let hashes = [
        bits(&dct),
        variant(|i, j| (j, i, !odd(j))),
        variant(|i, j| (i, j, odd(i + j))),
        variant(|i, j| (j, i, !odd(i))),
        variant(|i, j| (i, j, !odd(i))),
        variant(|i, j| (i, j, !odd(j))),
        variant(|i, j| (j, i, false)),
        variant(|i, j| (j, i, odd(i + j))),
    ];
    (hashes, quality)
}

/// One bit per coefficient, set when it is above the median.
fn bits(dct: &[f32; 256]) -> PdqHash {
    let mut sorted = *dct;
    sorted.sort_by(f32::total_cmp);
    // The 128th smallest of 256: the lower median, which the reference's
    // Torben median returns.
    let median = sorted[127];
    let mut hash = PdqHash::default();
    for (bit, &v) in dct.iter().enumerate() {
        if v > median {
            hash.set(bit);
        }
    }
    hash
}

/// The 16×16 DCT of the image and its quality, or `None` for an image too
/// small to hash.
fn pdq_dct(image: &DecodedImage) -> Option<([f32; 256], u32)> {
    let (mut luma, rows, cols) = luminance(image)?;
    if rows < MIN_SIDE || cols < MIN_SIDE {
        return None;
    }
    let small = if rows == 64 && cols == 64 {
        // Already 64×64: used as it is, as in the reference.
        let mut out = [0f32; 64 * 64];
        out.copy_from_slice(&luma);
        out
    } else {
        let mut tmp = vec![0f32; luma.len()];
        let along_rows = jarosz_window(cols);
        let along_cols = jarosz_window(rows);
        for _ in 0..2 {
            for r in 0..rows {
                box_1d(&luma, &mut tmp, r * cols, 1, cols, along_rows);
            }
            for c in 0..cols {
                box_1d(&tmp, &mut luma, c, cols, rows, along_cols);
            }
        }
        decimate(&luma, rows, cols)
    };
    Some((dct_16(&small), quality(&small)))
}

/// The image's luminance as rows × cols floats, after the [`RESIZE`] step.
fn luminance(image: &DecodedImage) -> Option<(Vec<f32>, usize, usize)> {
    let (w, h) = (image.width as usize, image.height as usize);
    if image.data.len() < w * h * 3 {
        return None;
    }
    let luma_at = |x: usize, y: usize| {
        let i = (y * w + x) * 3;
        let px = &image.data[i..i + 3];
        0.299f32 * f32::from(px[0]) + 0.587f32 * f32::from(px[1]) + 0.114f32 * f32::from(px[2])
    };
    // CImg's nearest-neighbour resize: output pixel k takes source pixel
    // ⌊k · size / RESIZE⌋ along each axis.
    let (rows, cols, xs, ys): (usize, usize, Vec<usize>, Vec<usize>) = if w > RESIZE || h > RESIZE {
        let pick = |n: usize| (0..RESIZE).map(|k| k * n / RESIZE).collect();
        (RESIZE, RESIZE, pick(w), pick(h))
    } else {
        (h, w, (0..w).collect(), (0..h).collect())
    };
    let mut out = Vec::with_capacity(rows * cols);
    for &y in &ys {
        for &x in &xs {
            out.push(luma_at(x, y));
        }
    }
    Some((out, rows, cols))
}

/// The Jarosz box-filter window for reducing `size` to 64.
fn jarosz_window(size: usize) -> usize {
    size.div_ceil(2 * 64)
}

/// A centered moving average of window `window` over `len` values of
/// `input` starting at `start`, `stride` apart, into the same positions of
/// `output`. The window shrinks at the ends. Written as the reference's
/// `box1DFloat`, down to the order of float operations.
fn box_1d(
    input: &[f32],
    output: &mut [f32],
    start: usize,
    stride: usize,
    len: usize,
    window: usize,
) {
    let half = (window + 2) / 2;
    let at = |i: usize| start + i * stride;
    let mut sum = 0f32;
    let mut count = 0usize;
    let (mut left, mut right, mut out) = (0usize, 0usize, 0usize);
    // Phase 1: fill the leading half window.
    for _ in 0..half.saturating_sub(1).min(len) {
        sum += input[at(right)];
        count += 1;
        right += 1;
    }
    // Phase 2: write while the window grows to full size.
    for _ in 0..(window + 1).saturating_sub(half) {
        if right >= len {
            break;
        }
        sum += input[at(right)];
        count += 1;
        right += 1;
        output[at(out)] = sum / count as f32;
        out += 1;
    }
    // Phase 3: slide the full window (add, then subtract, as the reference).
    while right < len {
        sum += input[at(right)];
        sum -= input[at(left)];
        right += 1;
        left += 1;
        output[at(out)] = sum / count as f32;
        out += 1;
    }
    // Phase 4: write while the window shrinks at the end.
    while out < len {
        sum -= input[at(left)];
        left += 1;
        count -= 1;
        output[at(out)] = sum / count.max(1) as f32;
        out += 1;
    }
}

/// Sample the filtered buffer at 64×64 evenly spaced points.
fn decimate(buf: &[f32], rows: usize, cols: usize) -> [f32; 64 * 64] {
    let mut out = [0f32; 64 * 64];
    for i in 0..64 {
        let r = (((i as f64 + 0.5) * rows as f64) / 64.0) as usize;
        for j in 0..64 {
            let c = (((j as f64 + 0.5) * cols as f64) / 64.0) as usize;
            out[i * 64 + j] = buf[r.min(rows - 1) * cols + c.min(cols - 1)];
        }
    }
    out
}

/// PDQ's quality: the summed (scaled) gradient between neighbouring pixels
/// of the 64×64 image, capped at 100.
fn quality(buf: &[f32; 64 * 64]) -> u32 {
    let mut sum: u64 = 0;
    let step = |u: f32, v: f32| (((u - v) * 100.0) / 255.0).abs() as u64;
    for i in 0..63 {
        for j in 0..64 {
            sum += step(buf[i * 64 + j], buf[(i + 1) * 64 + j]);
        }
    }
    for i in 0..64 {
        for j in 0..63 {
            sum += step(buf[i * 64 + j], buf[i * 64 + j + 1]);
        }
    }
    (sum / 90).min(100) as u32
}

/// The 16×16 low-frequency DCT of a 64×64 buffer, without the DC terms:
/// `D · A · Dᵀ` with `D[i][j] = √(2/64) · cos(π / 128 · (i + 1) · (2j + 1))`,
/// rounded and summed as in the reference.
fn dct_16(a: &[f32; 64 * 64]) -> [f32; 256] {
    // The reference keeps the scale as a float, then multiplies in double.
    let scale = (2.0f64 / 64.0).sqrt() as f32;
    let mut d = [[0f32; 64]; 16];
    for (i, row) in d.iter_mut().enumerate() {
        for (j, v) in row.iter_mut().enumerate() {
            let angle = std::f64::consts::PI / 2.0 / 64.0 * (i + 1) as f64 * (2 * j + 1) as f64;
            *v = (f64::from(scale) * angle.cos()) as f32;
        }
    }
    // T = D · A (16×64), each sum in order of k.
    let mut t = [[0f32; 64]; 16];
    for i in 0..16 {
        for j in 0..64 {
            let mut sum = 0f32;
            for k in 0..64 {
                sum += d[i][k] * a[k * 64 + j];
            }
            t[i][j] = sum;
        }
    }
    // B = T · Dᵀ (16×16).
    let mut b = [0f32; 256];
    for i in 0..16 {
        for j in 0..16 {
            let mut sum = 0f32;
            for k in 0..64 {
                sum += t[i][k] * d[j][k];
            }
            b[i * 16 + j] = sum;
        }
    }
    b
}

// -------------------------------- classifier --------------------------------

/// Default `max_distance`: Meta's suggested match threshold.
pub const DEFAULT_MAX_DISTANCE: u32 = 31;
/// Default `min_quality`: PDQ hashes below this are unreliable.
pub const DEFAULT_MIN_QUALITY: u32 = 50;
/// The side frames are extracted at for a video scan.
const INPUT_SIZE: u32 = 512;

/// One entry of a hash list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListEntry {
    pub hash: PdqHash,
    pub category: u32,
}

/// Parse a hash list: one `<hash> <category id>` per line; blank lines and
/// `#` comments (also after an entry) are ignored.
pub fn parse_hash_list(text: &str) -> Result<Vec<ListEntry>, String> {
    let mut out = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split_whitespace();
        let (Some(hash), Some(category), None) = (fields.next(), fields.next(), fields.next())
        else {
            return Err(format!("line {}: expected `<hash> <category id>`", n + 1));
        };
        let hash = hash.parse().map_err(|e| format!("line {}: {e}", n + 1))?;
        let category = category
            .parse()
            .map_err(|_| format!("line {}: '{category}' is not a category id", n + 1))?;
        out.push(ListEntry { hash, category });
    }
    Ok(out)
}

/// A block list as a classifier: each image's hash against every entry.
pub(crate) struct PdqClassifier {
    list: Vec<ListEntry>,
    /// The categories, by id.
    labels: Vec<Label>,
    max_distance: u32,
    min_quality: u32,
    /// Compare all 8 dihedral variants of each image (rotations, mirrors).
    rotations: bool,
}

/// The model's categories (`categories = { 1 = "csam" }`) as labels.
pub(crate) fn category_labels(cfg: &ModelConfig) -> Result<Vec<Label>, InferenceError> {
    cfg.categories
        .iter()
        .map(|(id, name)| {
            let id = id.parse().map_err(|_| {
                InferenceError::Config(format!("pdq category id '{id}' is not a number"))
            })?;
            Ok(Label {
                id,
                name: name.clone(),
            })
        })
        .collect()
}

pub(crate) fn load(
    cfg: &ModelConfig,
    files: &crate::ModelFiles,
) -> Result<PdqClassifier, InferenceError> {
    let path = cfg
        .hash_list
        .as_deref()
        .ok_or_else(|| InferenceError::Config("pdq models need a `hash_list`".into()))?;
    let text = files
        .hash_list
        .as_deref()
        .ok_or_else(|| InferenceError::Config(format!("hash list {path} could not be read")))?;
    let list = parse_hash_list(text).map_err(|e| InferenceError::Config(format!("{path}: {e}")))?;
    let labels = category_labels(cfg)?;
    if let Some(e) = list
        .iter()
        .find(|e| !labels.iter().any(|l| l.id == e.category))
    {
        return Err(InferenceError::Config(format!(
            "{path}: category {} is not in the model's `categories`",
            e.category
        )));
    }
    tracing::info!(hashes = list.len(), list = %path, "loaded pdq hash list");
    Ok(PdqClassifier {
        list,
        labels,
        max_distance: cfg.max_distance.unwrap_or(DEFAULT_MAX_DISTANCE),
        min_quality: cfg.min_quality.unwrap_or(DEFAULT_MIN_QUALITY),
        rotations: cfg.match_rotations.unwrap_or(true),
    })
}

impl PdqClassifier {
    /// The categories whose closest entry is within `max_distance` of any
    /// of `hashes` (an image's variants), with that distance.
    fn matches(&self, hashes: &[PdqHash]) -> BTreeMap<u32, u32> {
        let mut best: BTreeMap<u32, u32> = BTreeMap::new();
        for entry in &self.list {
            let d = hashes
                .iter()
                .map(|h| h.distance(&entry.hash))
                .min()
                .unwrap_or(u32::MAX);
            if d <= self.max_distance {
                let slot = best.entry(entry.category).or_insert(d);
                *slot = (*slot).min(d);
            }
        }
        best
    }
}

impl ImageClassifier for PdqClassifier {
    fn classify(&self, images: &[&DecodedImage]) -> Result<Vec<Classification>, InferenceError> {
        Ok(images
            .iter()
            .map(|img| {
                let (hashes, quality) = if self.rotations {
                    let (all, q) = pdq_dihedral_hashes(img);
                    (all.to_vec(), q)
                } else {
                    let (h, q) = pdq_hash(img);
                    (vec![h], q)
                };
                if quality < self.min_quality {
                    return Classification::default();
                }
                let mut predictions: Vec<Prediction> = self
                    .matches(&hashes)
                    .into_iter()
                    .map(|(category, d)| {
                        let name = self
                            .labels
                            .iter()
                            .find(|l| l.id == category)
                            .map_or_else(String::new, |l| l.name.clone());
                        Prediction::new(category, name, 1.0 - d as f32 / 256.0)
                    })
                    .collect();
                predictions.sort_by(|a, b| b.score.total_cmp(&a.score));
                Classification { predictions }
            })
            .collect())
    }

    fn labels(&self) -> &[Label] {
        &self.labels
    }

    fn input_size(&self) -> u32 {
        INPUT_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deterministic test picture: smooth gradients plus a few shapes,
    /// varied by `seed`.
    fn picture(width: u32, height: u32, seed: u32) -> DecodedImage {
        let mut data = Vec::with_capacity((width * height * 3) as usize);
        let s = seed as f32;
        for y in 0..height {
            for x in 0..width {
                let (u, v) = (x as f32 / width as f32, y as f32 / height as f32);
                let wave = ((u * (3.0 + s) + v * (2.0 + s * 0.7)) * std::f32::consts::PI).sin();
                let disc = if ((u - 0.3 - 0.05 * s).powi(2) + (v - 0.6).powi(2)) < 0.03 {
                    80.0
                } else {
                    0.0
                };
                let block = if u > 0.6 && v < 0.35 + 0.03 * s {
                    60.0
                } else {
                    0.0
                };
                let base = 90.0 + 70.0 * wave + disc - block + 40.0 * u;
                let r = (base + 30.0 * v).clamp(0.0, 255.0) as u8;
                let g = base.clamp(0.0, 255.0) as u8;
                let b = (base - 20.0 * u).clamp(0.0, 255.0) as u8;
                data.extend([r, g, b]);
            }
        }
        DecodedImage {
            width,
            height,
            data,
        }
    }

    /// A nearest-neighbour resize, standing in for a re-encoded copy.
    fn resized(img: &DecodedImage, width: u32, height: u32) -> DecodedImage {
        let mut data = Vec::with_capacity((width * height * 3) as usize);
        for y in 0..height {
            for x in 0..width {
                let sx = (x * img.width / width) as usize;
                let sy = (y * img.height / height) as usize;
                let i = (sy * img.width as usize + sx) * 3;
                data.extend_from_slice(&img.data[i..i + 3]);
            }
        }
        DecodedImage {
            width,
            height,
            data,
        }
    }

    /// Add deterministic per-pixel noise of ±`amount`.
    fn noisy(img: &DecodedImage, amount: i32) -> DecodedImage {
        let mut state = 12345u32;
        let data = img
            .data
            .iter()
            .map(|&v| {
                state = state.wrapping_mul(1_103_515_245).wrapping_add(12345);
                let delta = (state >> 16) as i32 % (2 * amount + 1) - amount;
                (i32::from(v) + delta).clamp(0, 255) as u8
            })
            .collect();
        DecodedImage {
            data,
            ..img.clone()
        }
    }

    /// An integer-only test pattern, so its pixels are the same everywhere.
    fn pattern(width: u32, height: u32) -> DecodedImage {
        let mut data = Vec::with_capacity((width * height * 3) as usize);
        for y in 0..height {
            for x in 0..width {
                let (u, v) = (x * 256 / width, y * 256 / height);
                let ring = ((u as i64 - 100).pow(2) + (v as i64 - 140).pow(2)) / 64;
                let block = if u > 160 && v < 90 { 70 } else { 0 };
                let base = (u + 2 * v + (ring % 64) as u32 + block) % 256;
                data.extend([
                    base as u8,
                    ((base * 3 + v) % 256) as u8,
                    ((base + 255 - u) % 256) as u8,
                ]);
            }
        }
        DecodedImage {
            width,
            height,
            data,
        }
    }

    /// Hashes from Meta's reference C++ implementation
    /// (facebook/ThreatExchange `pdq/cpp`, `pdqDihedralHash256esFromFloatLuma`,
    /// built without FMA contraction) of [`pattern`] at these sizes, after
    /// the same 512×512 resize for the larger one.
    const REFERENCE: [(u32, u32, &str); 4] = [
        (
            640,
            480,
            "672167424c1f3739371137f3b98efb1eff168c6e80c930fe67dc67a126429008",
        ),
        (
            900,
            600,
            "67216742441f3739371137f3b98efb1eff168c6e80c930fe77dc67a126429008",
        ),
        (
            64,
            64,
            "67a16768441a3b3d371127b3b18efb1eff14466648dd50f637dc37a922c39808",
        ),
        (
            100,
            60,
            "6f016760465ab71f371123f3b98efb1efb16462fc8e130fe27dc23a1374b8080",
        ),
    ];

    /// The reference's 8 dihedral hashes of `pattern(640, 480)`.
    const REFERENCE_DIHEDRAL: [&str; 8] = [
        "672167424c1f3739371137f3b98efb1eff168c6e80c930fe67dc67a126429008",
        "03e11e71df9fe064e3c00331fd8e2073063dbb859454c267337bdc2767d2439b",
        "3a74cde8bb4a9d976a449d59ecdb51b4aa4326c5dd9c9a542289cd0b631f1aa2",
        "56b4b6db8aca4acef695a99ba8db8ad95368112fc10168cd662e768d3287e931",
        "6f2198bd4e1fc8c63711c80cf98e04e1ff16739080c9cf0177dc985e764a4f77",
        "3a743a171b4a626c624462a6ecdbae4baa43d93ad59c65ab228932f4231fe5dd",
        "03e1e18cdf9e1f9123c0fccedd8edf8c063c447a94543d98337923d867d2bc24",
        "76b44b26aacab531feb55664a8db75265369eed0c1019732662e9972328716ce",
    ];

    #[test]
    fn hashes_match_metas_reference() {
        for (w, h, want) in REFERENCE {
            let (hash, quality) = pdq_hash(&pattern(w, h));
            assert_eq!(hash.to_string(), want, "{w}x{h}");
            assert_eq!(quality, 100, "{w}x{h}");
        }
        let (all, _) = pdq_dihedral_hashes(&pattern(640, 480));
        let got: Vec<String> = all.iter().map(PdqHash::to_string).collect();
        assert_eq!(got, REFERENCE_DIHEDRAL);
    }

    #[test]
    fn dihedral_hashes_are_the_hashes_of_the_rotated_images() {
        // Rotating the pixels 90° clockwise lands close to one of the
        // image's rotation variants (the reference calls it "rotate 270": it
        // turns the other way). Not exactly: the filter and the sampling grid
        // see the rotated pixels at a different phase.
        let img = pattern(240, 240);
        let mut rotated = Vec::with_capacity(img.data.len());
        for y in 0..240usize {
            for x in 0..240usize {
                // Clockwise: out(x, y) = in(y, 239 - x).
                let (sx, sy) = (y, 239 - x);
                let i = (sy * 240 + sx) * 3;
                rotated.extend_from_slice(&img.data[i..i + 3]);
            }
        }
        let rotated = DecodedImage {
            data: rotated,
            ..img.clone()
        };
        let (variants, _) = pdq_dihedral_hashes(&img);
        let (plain, _) = pdq_hash(&rotated);
        let distances: Vec<u32> = variants.iter().map(|v| v.distance(&plain)).collect();
        assert!(distances[3] <= DEFAULT_MAX_DISTANCE, "{distances:?}");
        for (i, d) in distances.iter().enumerate() {
            if i != 3 {
                assert!(*d > 60, "variant {i} is far: {distances:?}");
            }
        }
    }

    #[test]
    fn identical_images_hash_identically() {
        let a = picture(640, 480, 1);
        let (h1, q1) = pdq_hash(&a);
        let (h2, q2) = pdq_hash(&a.clone());
        assert_eq!(h1, h2);
        assert_eq!(h1.distance(&h2), 0);
        assert_eq!(q1, q2);
        assert!(q1 >= 50, "a detailed picture is good quality (got {q1})");
        // About half the bits are set (above the median).
        let ones: u32 = h1.0.iter().map(|w| w.count_ones()).sum();
        assert!((120..=128).contains(&ones), "{ones} bits set");
    }

    #[test]
    fn edited_copies_are_close_and_other_pictures_far() {
        let a = picture(640, 480, 1);
        let (h, _) = pdq_hash(&a);
        for (what, copy) in [
            ("resized", resized(&a, 320, 240)),
            ("upscaled", resized(&a, 1280, 960)),
            ("noisy", noisy(&a, 12)),
            ("squashed", resized(&a, 480, 480)),
        ] {
            let d = h.distance(&pdq_hash(&copy).0);
            assert!(d <= 20, "{what}: distance {d}");
        }
        for seed in 2..6 {
            let d = h.distance(&pdq_hash(&picture(640, 480, seed)).0);
            assert!(d >= 60, "seed {seed}: distance {d}");
        }
    }

    #[test]
    fn large_images_are_reduced_first() {
        let a = picture(2400, 1800, 3);
        let small = resized(&a, 600, 450);
        let d = pdq_hash(&a).0.distance(&pdq_hash(&small).0);
        assert!(d <= 20, "distance {d}");
    }

    #[test]
    fn flat_images_are_low_quality() {
        let flat = DecodedImage {
            width: 100,
            height: 80,
            data: vec![128; 100 * 80 * 3],
        };
        let (_, q) = pdq_hash(&flat);
        assert_eq!(q, 0);
        // Degenerate sizes do not panic.
        let one = DecodedImage {
            width: 1,
            height: 1,
            data: vec![0; 3],
        };
        pdq_hash(&one);
        let empty = DecodedImage {
            width: 0,
            height: 0,
            data: vec![],
        };
        assert_eq!(pdq_hash(&empty).1, 0);
    }

    #[test]
    fn hashes_round_trip_as_hex() {
        let (h, _) = pdq_hash(&picture(300, 200, 4));
        let hex = h.to_string();
        assert_eq!(hex.len(), 64);
        assert_eq!(hex.parse::<PdqHash>().unwrap(), h);
        // Most significant first: bit 255 is the first hex digit's top bit.
        let mut top = PdqHash::default();
        top.set(255);
        assert!(top.to_string().starts_with('8'));
        let mut bottom = PdqHash::default();
        bottom.set(0);
        assert!(bottom.to_string().ends_with('1'));
        assert!("xyz".parse::<PdqHash>().is_err());
        assert!("g".repeat(64).parse::<PdqHash>().is_err());
    }

    #[test]
    fn box_filter_matches_a_direct_moving_average() {
        let input: Vec<f32> = (0..20).map(|i| (i * i % 7) as f32).collect();
        for window in 1..8 {
            let mut out = vec![0f32; 20];
            box_1d(&input, &mut out, 0, 1, 20, window);
            // Reference: output i averages input[i - (window - half) ..= i + (half - 1)],
            // clipped to the ends, with half = (window + 2) / 2.
            let half = (window + 2) / 2;
            for (i, got) in out.iter().enumerate() {
                let lo = i.saturating_sub(window - half);
                let hi = (i + half - 1).min(19);
                let want: f32 = input[lo..=hi].iter().sum::<f32>() / (hi - lo + 1) as f32;
                assert!(
                    (got - want).abs() < 1e-4,
                    "window {window} at {i}: {got} vs {want}"
                );
            }
        }
    }

    #[test]
    fn lists_parse_with_comments_and_report_bad_lines() {
        let h = "f".repeat(64);
        let text = format!(
            "# block list\n{h} 1  # from case 7\n\n{} 2\n",
            "0".repeat(64)
        );
        let list = parse_hash_list(&text).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].category, 1);
        assert_eq!(list[1].hash, PdqHash::default());
        assert!(
            parse_hash_list(&format!("{h}\n"))
                .unwrap_err()
                .contains("line 1")
        );
        assert!(parse_hash_list("abc 1\n").is_err());
        assert!(parse_hash_list(&format!("{h} x\n")).is_err());
    }

    #[test]
    fn the_classifier_reports_matching_categories() {
        let a = picture(640, 480, 1);
        let (ha, _) = pdq_hash(&a);
        let (hb, _) = pdq_hash(&picture(640, 480, 5));
        let classifier = PdqClassifier {
            list: vec![
                ListEntry {
                    hash: ha,
                    category: 1,
                },
                ListEntry {
                    hash: hb,
                    category: 2,
                },
            ],
            labels: vec![
                Label {
                    id: 1,
                    name: "known".into(),
                },
                Label {
                    id: 2,
                    name: "other".into(),
                },
            ],
            max_distance: DEFAULT_MAX_DISTANCE,
            min_quality: DEFAULT_MIN_QUALITY,
            rotations: true,
        };
        let copy = resized(&a, 320, 240);
        let flat = DecodedImage {
            width: 64,
            height: 64,
            data: vec![10; 64 * 64 * 3],
        };
        let out = classifier
            .classify(&[&a, &copy, &picture(640, 480, 9), &flat])
            .unwrap();
        assert_eq!(out[0].predictions, [Prediction::new(1, "known", 1.0)]);
        assert_eq!(out[1].predictions.len(), 1);
        let p = &out[1].predictions[0];
        assert_eq!((p.label, p.name.as_str()), (1, "known"));
        assert!(p.score > 1.0 - 31.0 / 256.0 && p.score < 1.0, "{}", p.score);
        assert!(out[2].predictions.is_empty(), "{:?}", out[2]);
        assert!(out[3].predictions.is_empty(), "low quality never matches");
    }
}
