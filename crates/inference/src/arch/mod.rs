//! Architecture implementations — the seam for new model families. Each maps an
//! `Architecture` to a loader that produces an [`crate::ImageClassifier`], a
//! [`crate::TextClassifier`] or an [`crate::ImageToText`]. To add a family:
//! implement it here and add its arm to [`crate::load`].

pub(crate) mod paddleocr_vl;
pub(crate) mod pdq;
pub(crate) mod siglip;
pub(crate) mod text;
pub(crate) mod vit;
