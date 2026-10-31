//! PaddleOCR-VL (`paddleocr_vl`): reads the text in an image, with candle's
//! `paddleocr_vl` model — a NaViT-style vision encoder that takes an image at
//! its own shape, and ERNIE-4.5-0.3B writing out the text.
//!
//! - **Size:** an image keeps its shape and is scaled so each side is a
//!   multiple of 28 (a 14-pixel patch, merged 2×2) and its pixels fall in the
//!   model's range, capped by the config's `max_pixels` ([`fit`]).
//! - **Bands:** the model is made to read text regions, not whole scenes:
//!   given a busy video frame it missed a clear caption, or made text up,
//!   that it read at once from a band holding it. So an image is read in
//!   three overlapping horizontal bands, each half its height and a quarter
//!   apart (any line under a quarter of the height lies whole in one), at the
//!   scale the whole image would be read at. Images that read under
//!   [`BAND_MIN_HEIGHT`] pixels high are read whole, and so are images too
//!   small for half of them to keep that scale (a band is read at no fewer
//!   than the model's minimum pixels).
//! - **Prompt:** `User: <image>OCR:\nAssistant: `, as the model was trained.
//! - **Read:** greedy decoding until the end token or `max_tokens` (shared by
//!   an image's bands), one image at a time (the model's cache holds one
//!   sequence).
//! - **No text:** given an image without text, the model makes some up ("The
//!   quick brown fox…", "1", a note that the image holds no chart), unsure
//!   from its first token on. A read whose first token is less likely than
//!   `min_confidence` is dropped, and reading stops there.
//! - **Merging bands:** a line cut by a band's edge reads as a fragment or
//!   with garbled characters, which the model is unsure of. Copies of a line
//!   read in neighbouring bands are grouped ([`merge_lines`]), and each group
//!   gives one line: not a fragment of another copy, and of those the one
//!   whose least likely token is likelier.

use std::sync::Mutex;

use candle_core::{D, DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::paddleocr_vl::{Config, PaddleOCRVLModel};
use image::RgbImage;
use image::imageops::FilterType;
use tokenizers::Tokenizer;

use apollo_config::ModelConfig;
use apollo_domain::DecodedImage;

use crate::ImageToText;
use crate::error::InferenceError;
use crate::loader::Hub;

/// Pixels per patch side.
const PATCH: u32 = 14;
/// Sides are multiples of this: a patch, merged 2×2 into one image token.
const FACTOR: u32 = PATCH * 2;
/// The fewest pixels an image is read at (`preprocessor_config.json`).
const MIN_PIXELS: u64 = 147_384;
/// The most pixels the model was trained to read (`preprocessor_config.json`).
const MODEL_MAX_PIXELS: u64 = 2_822_400;
/// The default `max_pixels`: 768 image tokens, about 0.6 megapixels (1008×560
/// for a 16:9 frame). On captions, memes, chat screenshots, watermarks and
/// 22-pixel print on a 1080p frame it read what 1 megapixel did, in ~55% of
/// the time; at 0.4 megapixels digits and punctuation started to slip. The
/// model's own maximum (2.8 megapixels) is for dense documents.
pub(crate) const DEFAULT_MAX_PIXELS: u64 = 602_112;
/// The default `max_tokens`: a page of text. Reading stops at the end token
/// long before this for most images.
pub(crate) const DEFAULT_MAX_TOKENS: usize = 512;
/// The default `min_confidence`. Reading whole images, the made-up text of
/// images without text started at 0.03–0.11 and real text at 0.57–1.00;
/// small text on a busy photo, which the model barely reads, at 0.10–0.14
/// (from a band, at 0.9).
pub(crate) const DEFAULT_MIN_CONFIDENCE: f32 = 0.2;
/// Images reading under this many pixels high are read whole: cut into
/// bands, a short strip of text would only be cut through.
pub(crate) const BAND_MIN_HEIGHT: u32 = 16 * FACTOR;
/// Images whose sides differ by more than this factor are not read (the
/// model's limit).
const MAX_ASPECT: f64 = 200.0;
/// The task the model was trained with for plain text reading.
const TASK: &str = "OCR:";
/// Lines of different bands at least this similar are the same text.
const SAME_TEXT: f64 = 0.6;

/// The size an image of `width`×`height` is read at under `max_pixels`: its
/// shape kept, each side a multiple of 28, its area between the model's
/// minimum and `max_pixels`. `None` when the image is too long and thin for
/// the model (sides more than 200× apart).
pub(crate) fn fit(width: u32, height: u32, max_pixels: u64) -> Option<(u32, u32)> {
    if width == 0 || height == 0 {
        return None;
    }
    let (mut w, mut h) = (f64::from(width), f64::from(height));
    let factor = f64::from(FACTOR);
    // Thin images are first stretched to one patch row (or column).
    if h < factor {
        w = (w * factor / h).round();
        h = factor;
    }
    if w < factor {
        h = (h * factor / w).round();
        w = factor;
    }
    if w.max(h) / w.min(h) > MAX_ASPECT {
        return None;
    }
    let round = |x: f64| ((x / factor).round() * factor).max(factor);
    let (mut w_bar, mut h_bar) = (round(w), round(h));
    let max = max_pixels.max(MIN_PIXELS) as f64;
    if w_bar * h_bar > max {
        let beta = (w * h / max).sqrt();
        w_bar = ((w / beta / factor).floor() * factor).max(factor);
        h_bar = ((h / beta / factor).floor() * factor).max(factor);
    } else if w_bar * h_bar < MIN_PIXELS as f64 {
        let beta = (MIN_PIXELS as f64 / (w * h)).sqrt();
        w_bar = (w * beta / factor).ceil() * factor;
        h_bar = (h * beta / factor).ceil() * factor;
    }
    Some((w_bar as u32, h_bar as u32))
}

/// The `max_pixels` a model config reads at.
pub(crate) fn max_pixels(cfg: &ModelConfig) -> u64 {
    cfg.max_pixels
        .unwrap_or(DEFAULT_MAX_PIXELS)
        .min(MODEL_MAX_PIXELS)
}

/// Whether an image read at `width`×`height` (see [`fit`]) is read in bands:
/// when it is at least [`BAND_MIN_HEIGHT`] high, and half of it is at least
/// the model's minimum, so its bands are read at its scale (a smaller band
/// would be scaled up to the minimum).
fn banded(width: u32, height: u32) -> bool {
    height >= BAND_MIN_HEIGHT && u64::from(width) * u64::from(height) >= 2 * MIN_PIXELS
}

/// The rows of the bands an image `height` pixels high is read in: three,
/// each half its height, a quarter of it apart, as `(top, height)`.
fn bands(height: u32) -> [(u32, u32); 3] {
    let band = height.div_ceil(2);
    [(0, band), (height / 4, band), (height - band, band)]
}

/// A line of text read from one band of an image.
#[derive(Debug, Clone, PartialEq)]
struct Line {
    /// Which band it came from.
    band: usize,
    text: String,
    /// The probability of its least likely token.
    score: f32,
}

/// The lines of all bands (given band by band, each band's in reading order),
/// top to bottom, each line once. Neighbouring bands overlap, so a line there
/// is read twice, and a line cut by a band's edge reads as a fragment (slanted
/// text) or garbled (cut through its glyphs, read uncertainly).
///
/// - **Copies:** each band is aligned with the next ([`align`]): the lines
///   they share end the one band's list and start the next's, in the same
///   order.
/// - **Groups:** a line and its copies form a group, the strongest copies
///   joined first. A middle-band line is cut at one edge at most (a line is
///   shorter than the bands' quarter-height offset), so it is a cut copy in
///   one pair only. Two confident whole lines with different text are
///   different lines, so no group joins them.
/// - **Output:** each group gives its best copy (no fragment of a confident
///   longer one, then the best scored), placed in every band's own order.
///
/// A line that is a prefix or suffix (a third of it or more) of the next
/// band's edge line is taken for a fragment of it: "haha" just above a
/// "hahaha" that only lower bands read merges into it, as a slanted cut would
/// (its words stay in the text).
fn merge_lines(lines: Vec<Line>) -> Vec<String> {
    let key = |text: &str| -> Vec<char> {
        text.chars()
            .filter(|c| !c.is_whitespace())
            .flat_map(char::to_lowercase)
            .collect()
    };
    let lines: Vec<Line> = lines
        .into_iter()
        .filter(|l| !key(&l.text).is_empty())
        .collect();
    let keys: Vec<Vec<char>> = lines.iter().map(|l| key(&l.text)).collect();
    let confident = |i: usize| lines[i].score >= GARBLED;
    let bands = lines.iter().map(|l| l.band + 1).max().unwrap_or(0);
    let by_band: Vec<Vec<usize>> = (0..bands)
        .map(|b| (0..lines.len()).filter(|&i| lines[i].band == b).collect())
        .collect();

    // Copies between neighbouring bands: (line, line, weight, the cut copy).
    let mut copies: Vec<(usize, usize, f64, Option<usize>)> = Vec::new();
    for pair in by_band.windows(2) {
        let (upper, lower) = (&pair[0], &pair[1]);
        let seen = |band: &[usize]| -> Vec<Seen<'_>> {
            band.iter()
                .map(|&i| (keys[i].as_slice(), lines[i].score))
                .collect()
        };
        for (a, b, weight, cut) in align(&seen(upper), &seen(lower)) {
            let (a, b) = (upper[a], lower[b]);
            let cut = cut.map(|side| match side {
                Cut::Upper => a,
                Cut::Lower => b,
            });
            copies.push((a, b, weight, cut));
        }
    }
    // A middle-band line is cut at one edge at most: keep its best pair.
    let strongest_cut = |line: usize| {
        copies
            .iter()
            .filter(|c| c.3 == Some(line))
            .map(|c| c.2)
            .fold(f64::NEG_INFINITY, f64::max)
    };
    let mut copies: Vec<_> = copies
        .iter()
        .copied()
        .filter(|&(_, _, weight, cut)| {
            cut.is_none_or(|line| {
                lines[line].band == 0
                    || lines[line].band + 1 == bands
                    || weight >= strongest_cut(line)
            })
        })
        .collect();
    // Strongest first (a stable sort keeps the bands' order on ties).
    copies.sort_by(|x, y| y.2.total_cmp(&x.2));

    // A member is whole unless it is a fragment of a confident longer one.
    let whole = |m: usize, group: &[usize]| {
        !group.iter().any(|&o| {
            keys[m].len() < keys[o].len() && confident(o) && is_fragment(&keys[m], &keys[o])
        })
    };
    let mut group_of: Vec<usize> = (0..lines.len()).collect();
    let mut groups: Vec<Vec<usize>> = (0..lines.len()).map(|i| vec![i]).collect();
    for (a, b, _, _) in copies {
        let (ga, gb) = (group_of[a], group_of[b]);
        if ga != gb {
            let joined: Vec<usize> = groups[ga].iter().chain(&groups[gb]).copied().collect();
            let mut texts = joined
                .iter()
                .filter(|&&m| confident(m) && whole(m, &joined))
                .map(|&m| &keys[m]);
            let first = texts.next();
            if texts.any(|t| Some(t) != first) {
                continue;
            }
            let (keep, gone) = (ga.min(gb), ga.max(gb));
            for &m in &groups[gone] {
                group_of[m] = keep;
            }
            groups[keep] = joined;
            groups[gone].clear();
        }
    }

    // Place the groups: each band's order holds; where the bands leave a
    // choice, the line read in an upper band (or earlier in a band) first.
    let mut after: Vec<Vec<usize>> = vec![Vec::new(); lines.len()];
    let mut before = vec![0usize; lines.len()];
    for band in &by_band {
        for w in band.windows(2) {
            let (x, y) = (group_of[w[0]], group_of[w[1]]);
            if x != y && !after[x].contains(&y) {
                after[x].push(y);
                before[y] += 1;
            }
        }
    }
    let mut left: Vec<usize> = (0..lines.len())
        .filter(|&g| !groups[g].is_empty())
        .collect();
    let mut placed = Vec::with_capacity(left.len());
    while !left.is_empty() {
        // Groups are named by their first line, so the smallest that nothing
        // must precede (any, should the bands disagree).
        let at = left.iter().position(|&g| before[g] == 0).unwrap_or(0);
        let g = left.remove(at);
        for &next in &after[g] {
            before[next] = before[next].saturating_sub(1);
        }
        placed.push(g);
    }
    placed
        .into_iter()
        .map(|g| {
            let members = &groups[g];
            // The best scored whole copy; ties go to the earlier line.
            let best = members
                .iter()
                .copied()
                .filter(|&m| whole(m, members))
                .min_by(|&a, &b| lines[b].score.total_cmp(&lines[a].score).then(a.cmp(&b)))
                .unwrap_or(members[0]);
            lines[best].text.clone()
        })
        .collect()
}

/// A line as [`align`] sees it: its text (as compared) and its score.
type Seen<'a> = (&'a [char], f32);

/// Which copy of a pair a band's edge cut.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Cut {
    Upper,
    Lower,
}

/// A pair's weight as copies of one line, and its cut copy (none for the same
/// text).
type Pairing = (f64, Option<Cut>);

/// What a line inside the overlap of two bands costs an alignment when the
/// other band has no copy of it (missed, or cut beyond recognition). Above
/// half a copy's weight, so that skipping a line on each side never buys a
/// copy (a repeated name, "Bob", must not pair across real lines).
const MISSED: f64 = 1.5;

/// A copy whose least likely token is less likely than this reads garbled.
/// Lines cut through their glyphs by a band's edge scored 0.18–0.24 in our
/// tests; whole lines of real text never under 0.42.
const GARBLED: f32 = 0.35;

/// Whether `upper[i]` and `lower[j]` (neighbouring bands) are copies of one
/// line: its weight, and which copy is cut (none for the same text). The same
/// text weighs 3. Only a line at the edge where the overlap is cut
/// (`upper`'s last, `lower`'s first) can read imperfectly, so only there may
/// a copy be a fragment of the other (weight 1 + its share), or near-equal and
/// garbled (weight 1 + similarity). Elsewhere a look-alike ("Round 1",
/// "Round 2") is another line.
fn copy_weight(upper: &[Seen<'_>], lower: &[Seen<'_>], i: usize, j: usize) -> Option<Pairing> {
    let ((a, a_score), (b, b_score)) = (upper[i], lower[j]);
    if a == b {
        return Some((3.0, None));
    }
    let (at_bottom, at_top) = (i + 1 == upper.len(), j == 0);
    if at_bottom && is_fragment(a, b) {
        return Some((1.0 + a.len() as f64 / b.len() as f64, Some(Cut::Upper)));
    }
    if at_top && is_fragment(b, a) {
        return Some((1.0 + b.len() as f64 / a.len() as f64, Some(Cut::Lower)));
    }
    // The garbled copy is the less sure one, and must be at its edge.
    let (garbled, at_edge, side) = if a_score <= b_score {
        (a_score, at_bottom, Cut::Upper)
    } else {
        (b_score, at_top, Cut::Lower)
    };
    if !at_edge || garbled >= GARBLED {
        return None;
    }
    let sim = similarity(a, b);
    (sim >= SAME_TEXT).then_some((1.0 + sim, Some(side)))
}

/// The copies between the lines of a band (`upper`, in reading order) and of
/// the band below it (`lower`), as `(i, j, weight, cut)`. The bands overlap at
/// `upper`'s end and `lower`'s start, so the pairs are in order in both (two
/// lines' copies never cross), `upper`'s lines above its first copy and
/// `lower`'s below its last are free, and any other line without a copy
/// costs [`MISSED`]. The pairs ([`copy_weight`]) maximize their weight less
/// those costs; none when no pair is worth it. Linear in the pairs
/// considered: the costs are linear in the lines skipped, so the best earlier
/// alignment is a running maximum.
fn align(upper: &[Seen<'_>], lower: &[Seen<'_>]) -> Vec<(usize, usize, f64, Option<Cut>)> {
    type Pair = (usize, usize);
    // An alignment's score with its last pair (or the pair before it).
    type Step = (f64, Option<Pair>);
    let (n, m) = (upper.len(), lower.len());
    let mut weight: Vec<Vec<Option<Pairing>>> = vec![vec![None; m]; n];
    // For each pair: the best alignment ending with it, and the pair before.
    let mut cell: Vec<Vec<Option<Step>>> = vec![vec![None; m]; n];
    // For each pair: the best alignment ending at or before it in both bands,
    // its score raised by the cost of the lines before it (so continuing it
    // from any later pair subtracts one linear term), and its last pair.
    let mut prefix: Vec<Vec<Option<(f64, Pair)>>> = vec![vec![None; m]; n];
    let mut top: Step = (0.0, None);
    for i in 0..n {
        for j in 0..m {
            weight[i][j] = copy_weight(upper, lower, i, j);
            if let Some((w, _)) = weight[i][j] {
                // As the first pair: `lower`'s lines above `j` have no copy.
                let mut base: Step = (-MISSED * j as f64, None);
                if i > 0
                    && j > 0
                    && let Some((k, at)) = prefix[i - 1][j - 1]
                {
                    // The lines between the two pairs have no copy.
                    let go_on = k - MISSED * (i + j - 2) as f64;
                    if go_on > base.0 {
                        base = (go_on, Some(at));
                    }
                }
                let here = base.0 + w;
                cell[i][j] = Some((here, base.1));
                // As the last pair: `upper`'s lines below `i` have no copy.
                let total = here - MISSED * (n - 1 - i) as f64;
                if total > top.0 {
                    top = (total, Some((i, j)));
                }
            }
            let own = cell[i][j].map(|(score, _)| (score + MISSED * (i + j) as f64, (i, j)));
            let up = if i > 0 { prefix[i - 1][j] } else { None };
            let left = if j > 0 { prefix[i][j - 1] } else { None };
            prefix[i][j] = [own, up, left]
                .into_iter()
                .flatten()
                .reduce(|a, b| if b.0 > a.0 { b } else { a });
        }
    }
    let mut pairs = Vec::new();
    let mut at = top.1;
    while let Some((i, j)) = at {
        let (w, cut) = weight[i][j].expect("a pair has a weight");
        pairs.push((i, j, w, cut));
        at = cell[i][j].and_then(|(_, prev)| prev);
    }
    pairs.reverse();
    pairs
}

/// Whether `part` is what a band's edge leaves of `line` when it cuts through
/// slanted text: its start or its end, at least a third of it (a short word
/// that merely ends a longer line, like "lol", is a line of its own).
fn is_fragment(part: &[char], line: &[char]) -> bool {
    part.len() <= line.len()
        && part.len() * 3 >= line.len()
        && (line.starts_with(part) || line.ends_with(part))
}

/// 1 minus the edit distance over the longer length: 1 for equal strings.
fn similarity(a: &[char], b: &[char]) -> f64 {
    let longest = a.len().max(b.len());
    if longest == 0 {
        return 1.0;
    }
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    let mut current = vec![0; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        current[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let substitution = previous[j] + usize::from(ca != cb);
            current[j + 1] = substitution.min(previous[j + 1] + 1).min(current[j] + 1);
        }
        std::mem::swap(&mut previous, &mut current);
    }
    1.0 - previous[b.len()] as f64 / longest as f64
}

/// A loaded PaddleOCR-VL model.
pub(crate) struct PaddleOcrVl {
    /// Generation keeps a cache of the sequence, so one image at a time.
    model: Mutex<PaddleOCRVLModel>,
    tokenizer: Tokenizer,
    device: Device,
    dtype: DType,
    max_pixels: u64,
    max_tokens: usize,
    /// A read whose first token is less likely than this is dropped.
    min_confidence: f32,
    /// Token ids before the image's tokens (`<bos>User: <vision start>`)
    /// and after them (`<vision end>OCR:\nAssistant: `).
    before_image: Vec<u32>,
    after_image: Vec<u32>,
    image_token: u32,
    end_token: u32,
}

/// Load PaddleOCR-VL from its Hub repo.
pub(crate) fn load(
    hub: &Hub,
    cfg: &ModelConfig,
    device: &Device,
) -> Result<PaddleOcrVl, InferenceError> {
    let config: Config = serde_json::from_slice(&std::fs::read(hub.file("config.json")?)?)
        .map_err(|e| InferenceError::Config(format!("config.json: {e}")))?;
    let tokenizer = Tokenizer::from_file(hub.file("tokenizer.json")?)
        .map_err(|e| InferenceError::Config(format!("tokenizer.json: {e}")))?;
    let weights = hub.file("model.safetensors")?;
    // The weights are bf16; they are read as f32, which every device runs
    // and which reads the same as the reference implementation.
    let dtype = DType::F32;
    // SAFETY: mmap of a trusted, locally-cached safetensors file.
    let vb = unsafe { VarBuilder::from_mmaped_safetensors(&[weights], dtype, device)? };
    let model = PaddleOCRVLModel::new(&config, vb)?;

    let token = |name: &str| {
        tokenizer
            .token_to_id(name)
            .ok_or_else(|| InferenceError::Config(format!("the tokenizer has no '{name}' token")))
    };
    let encode = |text: &str| {
        tokenizer
            .encode(text, false)
            .map(|e| e.get_ids().to_vec())
            .map_err(|e| InferenceError::Config(format!("tokenizer: {e}")))
    };
    let mut before_image = vec![token("<|begin_of_sentence|>")?];
    before_image.extend(encode("User: ")?);
    before_image.push(config.vision_start_token_id);
    let mut after_image = vec![config.vision_end_token_id];
    after_image.extend(encode(TASK)?);
    after_image.extend(encode("\nAssistant: ")?);

    Ok(PaddleOcrVl {
        model: Mutex::new(model),
        tokenizer: tokenizer.clone(),
        device: device.clone(),
        dtype,
        max_pixels: max_pixels(cfg),
        max_tokens: cfg.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
        min_confidence: cfg.min_confidence.unwrap_or(DEFAULT_MIN_CONFIDENCE),
        before_image,
        after_image,
        image_token: config.image_token_id,
        end_token: token("</s>")?,
    })
}

/// What one read gave: its tokens with their probabilities, and the first
/// token's probability (1 with none).
struct Read {
    tokens: Vec<(u32, f32)>,
    confidence: f32,
}

impl PaddleOcrVl {
    /// The text in an image: read in bands, or whole when it is short.
    fn read_image(&self, image: &DecodedImage) -> Result<String, InferenceError> {
        let (width, height) = fit(image.width, image.height, self.max_pixels).ok_or_else(|| {
            InferenceError::Preprocess(format!(
                "a {}x{} image is too long and thin to read (sides more than {MAX_ASPECT}x apart)",
                image.width, image.height
            ))
        })?;
        if !banded(width, height) {
            let read =
                self.read_region(image, self.max_pixels, self.max_tokens, self.min_confidence)?;
            return self.decode(&read.tokens);
        }
        let row = image.width as usize * 3;
        let mut lines = Vec::new();
        // `max_tokens` is for the whole image: what a band reads, the next
        // band has less of.
        let mut budget = self.max_tokens;
        for (band, (top, rows)) in bands(image.height).into_iter().enumerate() {
            if budget == 0 {
                break;
            }
            let (top, rows) = (top as usize, rows as usize);
            let region = DecodedImage {
                width: image.width,
                height: rows as u32,
                data: image.data[top * row..(top + rows) * row].to_vec(),
            };
            // Half the image, so half its pixels: the same scale.
            let read =
                self.read_region(&region, self.max_pixels / 2, budget, self.min_confidence)?;
            budget = budget.saturating_sub(read.tokens.len());
            lines.extend(self.lines(band, &read.tokens)?);
        }
        Ok(merge_lines(lines).join("\n"))
    }

    /// The text of `tokens`, trimmed.
    fn decode(&self, tokens: &[(u32, f32)]) -> Result<String, InferenceError> {
        let ids: Vec<u32> = tokens.iter().map(|&(t, _)| t).collect();
        let text = self
            .tokenizer
            .decode(&ids, true)
            .map_err(|e| InferenceError::Preprocess(format!("decoding the text: {e}")))?;
        Ok(text.trim().to_string())
    }

    /// The lines of a band's read, each scored by its least likely token.
    fn lines(&self, band: usize, tokens: &[(u32, f32)]) -> Result<Vec<Line>, InferenceError> {
        let mut lines = Vec::new();
        let mut start = 0;
        for (i, &(token, _)) in tokens.iter().enumerate() {
            let piece = self.tokenizer.decode(&[token], false).unwrap_or_default();
            if piece.contains('\n') || i + 1 == tokens.len() {
                let line = &tokens[start..=i];
                let text = self.decode(line)?;
                if !text.is_empty() {
                    lines.extend(text.lines().map(|l| Line {
                        band,
                        text: l.trim().to_string(),
                        score: line.iter().map(|&(_, p)| p).fold(1.0, f32::min),
                    }));
                }
                start = i + 1;
            }
        }
        lines.retain(|l| !l.text.is_empty());
        Ok(lines)
    }

    /// Read one image (or band), at up to `max_pixels`, writing at most
    /// `max_tokens` tokens; none when the first is less likely than
    /// `min_confidence`.
    fn read_region(
        &self,
        image: &DecodedImage,
        max_pixels: u64,
        max_tokens: usize,
        min_confidence: f32,
    ) -> Result<Read, InferenceError> {
        let (width, height) = fit(image.width, image.height, max_pixels).ok_or_else(|| {
            InferenceError::Preprocess(format!(
                "a {}x{} image is too long and thin to read (sides more than {MAX_ASPECT}x apart)",
                image.width, image.height
            ))
        })?;
        let pixels = self.pixels(image, width, height)?;
        let grid = Tensor::new(&[[1u32, height / PATCH, width / PATCH]], &self.device)?;
        let image_tokens = ((width / FACTOR) * (height / FACTOR)) as usize;
        let mut ids =
            Vec::with_capacity(self.before_image.len() + image_tokens + self.after_image.len());
        ids.extend_from_slice(&self.before_image);
        ids.extend(std::iter::repeat_n(self.image_token, image_tokens));
        ids.extend_from_slice(&self.after_image);
        let ids = Tensor::new(ids.as_slice(), &self.device)?.unsqueeze(0)?;

        let read = {
            let mut model = self
                .model
                .lock()
                .map_err(|_| InferenceError::Unsupported("the model is poisoned".into()))?;
            self.generate(
                &mut model,
                &ids,
                (&pixels, &grid),
                max_tokens.max(1),
                min_confidence,
            )?
        };
        tracing::debug!(
            width,
            height,
            tokens = read.tokens.len(),
            confidence = read.confidence,
            "read an image"
        );
        Ok(read)
    }

    /// Greedy decoding: the image and prompt in one pass, then one token at a
    /// time until the end token (left out) or `max_tokens`, none when the
    /// first is less likely than `min_confidence` (the model being unsure
    /// there is any text).
    fn generate(
        &self,
        model: &mut PaddleOCRVLModel,
        ids: &Tensor,
        (pixels, grid): (&Tensor, &Tensor),
        max_tokens: usize,
        min_confidence: f32,
    ) -> Result<Read, InferenceError> {
        model.clear_kv_cache();
        let mut logits = model.forward(ids, Some(pixels), Some(grid), 0)?;
        let mut offset = ids.dim(1)?;
        let mut tokens = Vec::new();
        let mut confidence = 1.0;
        loop {
            // The likeliest token and its probability, in one read back.
            let probs = candle_nn::ops::softmax_last_dim(&logits.to_dtype(DType::F32)?)?;
            let best = Tensor::cat(
                &[
                    probs.max_keepdim(D::Minus1)?,
                    probs.argmax_keepdim(D::Minus1)?.to_dtype(DType::F32)?,
                ],
                D::Minus1,
            )?
            .flatten_all()?
            .to_vec1::<f32>()?;
            let (p, token) = (best[0], best[1] as u32);
            if token == self.end_token {
                break;
            }
            if tokens.is_empty() {
                confidence = p;
                if p < min_confidence {
                    break;
                }
            }
            tokens.push((token, p));
            if tokens.len() == max_tokens {
                break;
            }
            let next = Tensor::new(&[token], &self.device)?.unsqueeze(0)?;
            logits = model.forward(&next, None, None, offset)?;
            offset += 1;
        }
        model.clear_kv_cache();
        Ok(Read { tokens, confidence })
    }

    /// The image at `width`×`height` (bicubic, as the reference processor),
    /// scaled to [-1, 1], channels first: (1, 3, height, width).
    fn pixels(
        &self,
        image: &DecodedImage,
        width: u32,
        height: u32,
    ) -> Result<Tensor, InferenceError> {
        let rgb = RgbImage::from_raw(image.width, image.height, image.data.clone())
            .ok_or_else(|| InferenceError::Preprocess("image buffer has the wrong size".into()))?;
        let rgb = if (rgb.width(), rgb.height()) == (width, height) {
            rgb
        } else {
            image::imageops::resize(&rgb, width, height, FilterType::CatmullRom)
        };
        let plane = (width * height) as usize;
        let mut data = vec![0f32; 3 * plane];
        for (i, px) in rgb.pixels().enumerate() {
            for c in 0..3 {
                data[c * plane + i] = f32::from(px[c]) / 127.5 - 1.0;
            }
        }
        let t = Tensor::from_vec(data, (1, 3, height as usize, width as usize), &self.device)?;
        Ok(t.to_dtype(self.dtype)?)
    }
}

impl ImageToText for PaddleOcrVl {
    fn read(&self, images: &[&DecodedImage]) -> Result<Vec<String>, InferenceError> {
        images.iter().map(|img| self.read_image(img)).collect()
    }

    fn warm_up(&self) -> Result<(), InferenceError> {
        let blank = DecodedImage {
            width: FACTOR,
            height: FACTOR,
            data: vec![255; (FACTOR * FACTOR * 3) as usize],
        };
        // Two tokens, whatever their confidence: the image pass and one step
        // after it.
        self.read_region(&blank, self.max_pixels, 2, 0.0).map(drop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fit_keeps_the_shape_on_the_patch_grid() {
        // Within range: each side rounded to a multiple of 28.
        assert_eq!(fit(640, 480, DEFAULT_MAX_PIXELS), Some((644, 476)));
        assert_eq!(fit(1920, 1080, DEFAULT_MAX_PIXELS), Some((1008, 560)));
        // Too big: scaled down to fit, keeping the shape.
        let (w, h) = fit(1920, 1080, 1_000_000).unwrap();
        assert_eq!((w % 28, h % 28), (0, 0));
        assert!(u64::from(w * h) <= 1_000_000, "{w}x{h}");
        assert!((f64::from(w) / f64::from(h) - 16.0 / 9.0).abs() < 0.05);
        // Too small: scaled up to the model's minimum.
        let (w, h) = fit(28, 28, DEFAULT_MAX_PIXELS).unwrap();
        assert_eq!((w, h), (392, 392));
        assert!(u64::from(w * h) >= MIN_PIXELS);
        // A 1-pixel-high strip is stretched to a patch row first.
        assert!(fit(100, 1, DEFAULT_MAX_PIXELS).is_some());
        // Too long and thin, or empty.
        assert_eq!(fit(30_000, 100, DEFAULT_MAX_PIXELS), None);
        assert_eq!(fit(0, 10, DEFAULT_MAX_PIXELS), None);
    }

    #[test]
    fn max_pixels_is_capped_at_the_models_maximum() {
        let mut cfg = apollo_config::load::from_str(
            "[models.ocr]\narchitecture = \"paddleocr_vl\"\nrepo = \"PaddlePaddle/PaddleOCR-VL\"",
        )
        .unwrap()
        .models
        .remove("ocr")
        .unwrap();
        assert_eq!(max_pixels(&cfg), DEFAULT_MAX_PIXELS);
        cfg.max_pixels = Some(10_000_000);
        assert_eq!(max_pixels(&cfg), MODEL_MAX_PIXELS);
        let (w, h) = fit(4000, 3000, max_pixels(&cfg)).unwrap();
        assert!(u64::from(w * h) <= MODEL_MAX_PIXELS);
    }

    #[test]
    fn bands_overlap_so_every_short_line_lies_whole_in_one() {
        for height in [448, 449, 676, 1080, 2081] {
            let b = bands(height);
            assert_eq!(b[0].0, 0);
            assert_eq!(b[2].0 + b[2].1, height, "the last band reaches the bottom");
            // A line under a quarter of the height, anywhere, fits in a band.
            let line = height / 4 - 1;
            for top in 0..=height - line {
                assert!(
                    b.iter().any(|&(t, h)| t <= top && top + line <= t + h),
                    "{height}: a line at {top} fits no band"
                );
            }
        }
    }

    fn line(band: usize, text: &str, score: f32) -> Line {
        Line {
            band,
            text: text.into(),
            score,
        }
    }

    #[test]
    fn merging_drops_cut_fragments_and_garbled_repeats() {
        // A line across two band edges: read whole by the middle band, cut
        // into a prefix and a suffix by the others.
        let rotated = vec![
            line(0, "STOP SPA", 0.24),
            line(1, "STOP SPAMMING ME", 0.65),
            line(2, "SPAMMING ME", 0.53),
        ];
        assert_eq!(merge_lines(rotated), ["STOP SPAMMING ME"]);
        // Read whole twice and garbled once, where a band cut the glyphs.
        let cut = vec![
            line(0, "你好世界 欢迎光临", 0.86),
            line(1, "你好世界 欢迎光临", 0.96),
            line(2, "你好世界欢迎无仙", 0.18),
        ];
        assert_eq!(merge_lines(cut), ["你好世界 欢迎光临"]);
        // Fragments scored above their whole line still give way to it.
        let fragments_first = vec![
            line(0, "STOP SP", 0.98),
            line(1, "STOP SPAMMING ME", 0.65),
            line(2, "MING ME", 0.97),
        ];
        assert_eq!(merge_lines(fragments_first), ["STOP SPAMMING ME"]);
        // Different lines stay, top to bottom; a line in the overlap of two
        // bands is read by both and kept once; a repeat in one band, or in
        // the top and bottom bands (which do not overlap), is another line.
        let meme = vec![
            line(0, "LOL", 0.9),
            line(0, "hey are you coming tonight?", 0.9),
            line(1, "hey are you coming tonight?", 0.9),
            line(1, "no, you are so stupid lol", 0.8),
            line(2, "ok see you at 8pm then", 0.9),
            line(2, "LOL", 0.9),
            line(2, "LOL", 0.9),
        ];
        assert_eq!(
            merge_lines(meme),
            [
                "LOL",
                "hey are you coming tonight?",
                "no, you are so stupid lol",
                "ok see you at 8pm then",
                "LOL",
                "LOL",
            ]
        );
    }

    #[test]
    fn merging_does_not_depend_on_which_copy_scores_best() {
        // The middle band's copy loses to the top band's; the bottom band's
        // garbled copy still matches it through the middle one.
        let cut = vec![
            line(0, "你好世界 欢迎光临", 0.97),
            line(1, "你好世界 欢迎光临", 0.96),
            line(2, "你好世界欢迎无仙", 0.18),
        ];
        assert_eq!(merge_lines(cut), ["你好世界 欢迎光临"]);
        let suffix = vec![
            line(0, "STOP SPAMMING ME", 0.9),
            line(1, "STOP SPAMMING ME", 0.8),
            line(2, "SPAMMING ME", 0.7),
        ];
        assert_eq!(merge_lines(suffix), ["STOP SPAMMING ME"]);
        // A line read in the overlap stays in its place, whichever copy wins:
        // here the middle band's "bravo" outscores the top band's.
        let order = vec![
            line(0, "alpha one", 0.9),
            line(0, "bravo two", 0.5),
            line(0, "charlie three", 0.9),
            line(1, "bravo two", 0.9),
            line(1, "charlie three", 0.6),
            line(1, "delta four", 0.9),
        ];
        assert_eq!(
            merge_lines(order),
            ["alpha one", "bravo two", "charlie three", "delta four"]
        );
    }

    #[test]
    fn look_alike_lines_keep_their_own_copies() {
        // Two different but similar lines, both read in the overlap; the
        // crossed pairing scores best line by line, the order does not allow it.
        let rounds = vec![
            line(0, "Round 1", 0.90),
            line(0, "Round 2", 0.99),
            line(1, "Round 1", 0.98),
            line(1, "Round 2", 0.95),
        ];
        assert_eq!(merge_lines(rounds), ["Round 1", "Round 2"]);
        // A short line above a longer one it starts.
        let ok = vec![
            line(0, "ok", 0.9),
            line(0, "ok bye", 0.9),
            line(1, "ok bye", 0.9),
        ];
        assert_eq!(merge_lines(ok), ["ok", "ok bye"]);
        let times = vec![
            line(0, "ok see you at 8pm", 0.9),
            line(0, "ok see you at 9pm", 0.9),
            line(1, "ok see you at 9pm", 0.95),
        ];
        assert_eq!(
            merge_lines(times),
            ["ok see you at 8pm", "ok see you at 9pm"]
        );
    }

    /// A list read perfectly in three bands of `per_band` lines each, each
    /// band starting `step` lines below the one above it.
    fn list(texts: &[String], per_band: usize, step: usize) -> Vec<Line> {
        (0..3)
            .flat_map(|band| {
                texts
                    .iter()
                    .skip(band * step)
                    .take(per_band)
                    .map(move |t| line(band, t, 0.9))
            })
            .collect()
    }

    #[test]
    fn lists_of_look_alike_lines_lose_none() {
        let numbered = |prefix: &str, from: usize, to: usize| -> Vec<String> {
            (from..=to).map(|k| format!("{prefix}{k}")).collect()
        };
        // One line shared: Day 3 in the top two bands, Day 5 in the lower two.
        let days = numbered("Day ", 1, 7);
        assert_eq!(merge_lines(list(&days, 3, 2)), days[..7].to_vec());
        // Dense: four lines shared between neighbouring bands.
        let orders = numbered("Order #", 1001, 1016);
        assert_eq!(merge_lines(list(&orders, 8, 4)), orders);
        let episodes = numbered("Episode ", 1, 8);
        assert_eq!(merge_lines(list(&episodes, 4, 2)), episodes);
        // Prefixes of each other, one shared per overlap.
        let hello = vec![
            line(0, "hello", 0.9),
            line(0, "hello world", 0.9),
            line(1, "hello world", 0.9),
            line(1, "hello world again", 0.9),
            line(2, "hello world again", 0.9),
        ];
        assert_eq!(
            merge_lines(hello),
            ["hello", "hello world", "hello world again"]
        );
    }

    #[test]
    fn repeated_names_do_not_pair_across_real_lines() {
        let l = |band, text| line(band, text, 0.9);
        let chat = vec![
            l(0, "Alice"),
            l(0, "anyone up?"),
            l(0, "Bob"),
            l(0, "yeah"),
            l(0, "Alice"),
            l(0, "lets play"),
            l(0, "Bob"),
            l(1, "Alice"),
            l(1, "lets play"),
            l(1, "Bob"),
            l(1, "what game"),
            l(1, "Alice"),
            l(1, "valorant"),
            l(1, "Bob"),
            l(2, "Alice"),
            l(2, "valorant"),
            l(2, "Bob"),
            l(2, "gg"),
        ];
        assert_eq!(
            merge_lines(chat),
            [
                "Alice",
                "anyone up?",
                "Bob",
                "yeah",
                "Alice",
                "lets play",
                "Bob",
                "what game",
                "Alice",
                "valorant",
                "Bob",
                "gg"
            ]
        );
        // A garbled cut line pairs with its own copy, not with a repeated
        // name across the lines in between.
        let cut = vec![
            l(0, "SALE"),
            line(0, "50% off everythinq", 0.2),
            l(1, "50% off everything"),
            l(1, "SALE"),
            line(2, "5O% off everythinq", 0.2),
            l(2, "SALE"),
        ];
        assert_eq!(merge_lines(cut), ["SALE", "50% off everything", "SALE"]);
    }

    #[test]
    fn a_cut_line_is_one_lines_copy() {
        // The middle band's only line is the cut lower caption; it cannot
        // also be the upper one's copy.
        let meme = vec![
            line(0, "ME AT 9AM", 0.93),
            line(1, "ME AT 9PN", 0.21),
            line(2, "ME AT 9PM", 0.91),
        ];
        assert_eq!(merge_lines(meme), ["ME AT 9AM", "ME AT 9PM"]);
        // A band's only line, read with one uncertain token and no copy, is
        // kept: where it lies in the band is not known.
        let caption = vec![line(2, "you are worthless garbage", 0.3)];
        assert_eq!(merge_lines(caption), ["you are worthless garbage"]);
        // A garbled copy does not win for being longer.
        let day = vec![
            line(0, "Total", 0.9),
            line(0, "Day 1.", 0.2),
            line(1, "Day 1", 0.9),
            line(1, "Day 2", 0.9),
        ];
        assert_eq!(merge_lines(day), ["Total", "Day 1", "Day 2"]);
        // Ambiguous: a line that starts the next band's first line reads as
        // its slanted fragment, and merges into it (its words stay).
        let haha = vec![
            line(0, "look at my cat", 0.9),
            line(0, "haha", 0.9),
            line(1, "hahaha", 0.9),
            line(2, "hahaha", 0.9),
        ];
        assert_eq!(merge_lines(haha), ["look at my cat", "hahaha"]);
    }

    #[test]
    fn repeated_lines_stay_where_they_are() {
        // "LOL" above "hello there" in the top band only, and below it in the
        // overlap of the lower bands: two lines, not one.
        let lol = vec![
            line(0, "LOL", 0.9),
            line(0, "hello there", 0.9),
            line(1, "hello there", 0.9),
            line(1, "LOL", 0.9),
            line(2, "LOL", 0.9),
        ];
        assert_eq!(merge_lines(lol), ["LOL", "hello there", "LOL"]);
        // A line only the middle band read goes where it read it: above the
        // line that follows it there.
        let missed = vec![
            line(0, "alpha one", 0.9),
            line(0, "charlie three", 0.9),
            line(1, "bravo two", 0.9),
            line(1, "charlie three", 0.9),
        ];
        assert_eq!(
            merge_lines(missed),
            ["alpha one", "bravo two", "charlie three"]
        );
    }

    #[test]
    fn small_images_are_read_whole() {
        // Banded: tall enough, and half of it is at least the model's minimum.
        assert!(banded(1008, 560));
        assert!(banded(644, 476));
        // At the limits: exactly 448 high, and just above twice the minimum.
        assert!(banded(672, 448));
        assert!(banded(224, 1316));
        // Too short, or too small for a band to keep the image's scale.
        assert!(!banded(1316, 420));
        assert!(!banded(336, 448));
        assert!(!banded(224, 1288));
        let (w, h) = fit(1080, 1920, 150_000).unwrap();
        assert!(!banded(w, h), "{w}x{h}");
    }

    #[test]
    fn similarity_is_one_minus_the_edit_distance_share() {
        let chars = |s: &str| s.chars().collect::<Vec<_>>();
        assert_eq!(similarity(&chars("abc"), &chars("abc")), 1.0);
        assert_eq!(similarity(&chars("abcd"), &chars("abxd")), 0.75);
        assert_eq!(similarity(&chars(""), &chars("ab")), 0.0);
        assert!(is_fragment(&chars("stopsp"), &chars("stopspamming")));
        assert!(is_fragment(&chars("mming"), &chars("stopspamming")));
        // Too short a share, not at an end, or longer than the line.
        assert!(!is_fragment(&chars("lol"), &chars("youaresostupidlol")));
        assert!(!is_fragment(&chars("pammi"), &chars("stopspamming")));
        assert!(!is_fragment(&chars("stopspamming"), &chars("stop")));
    }
}
