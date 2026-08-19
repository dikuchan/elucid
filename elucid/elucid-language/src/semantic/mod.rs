//! Semantic analysis: AST → IR conversion.
//!
//! This module provides conversion from parsed AST nodes into the
//! intermediate representation (IR).

mod command;
mod expression;
mod pipeline;

#[cfg(test)]
mod relation_tests;

pub(crate) mod error;

pub use error::{AnalyzeError, ParseError, SemanticError};

pub(crate) use pipeline::convert_query;
