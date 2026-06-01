//! Semantic analysis: AST → IR conversion.
//!
//! This module provides conversion from parsed AST nodes into the
//! intermediate representation (IR). Expression conversion is infallible;
//! command conversion may produce [`SemanticError`] values for structural
//! validation failures.

pub(crate) mod error;
mod command;
mod expression;
mod pipeline;
mod validate;

pub use error::{AnalyzeError, ParseError, SemanticError};

pub(crate) use pipeline::convert_query;
