//! Architecture implementations — the seam for new model families. Each maps an
//! `Architecture` to a loader that produces an [`crate::ImageClassifier`] or a
//! [`crate::VideoClassifier`]. To add a family: implement it here and add its arm
//! to [`crate::load`].

pub(crate) mod videomae;
pub(crate) mod vit;
