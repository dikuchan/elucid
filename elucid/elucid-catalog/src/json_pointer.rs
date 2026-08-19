use std::fmt::{Display, Formatter, Write};
use std::str::FromStr;

use crate::CatalogModelError;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct JsonPointerToken(String);

impl JsonPointerToken {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct JsonPointer {
    tokens: Vec<JsonPointerToken>,
}

impl JsonPointer {
    pub fn parse(value: &str) -> Result<Self, CatalogModelError> {
        if value.is_empty() {
            return Ok(Self::from_tokens(Vec::new()));
        }
        let encoded_tokens = value
            .strip_prefix('/')
            .ok_or(CatalogModelError::JsonPointerMustStartWithSlash)?;
        let tokens = encoded_tokens
            .split('/')
            .enumerate()
            .map(|(token_index, token)| decode_token(token, token_index))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::from_tokens(tokens))
    }

    #[must_use]
    pub fn from_tokens(tokens: Vec<JsonPointerToken>) -> Self {
        Self { tokens }
    }

    #[must_use]
    pub fn tokens(&self) -> &[JsonPointerToken] {
        &self.tokens
    }

    #[must_use]
    pub fn is_root(&self) -> bool {
        self.tokens.is_empty()
    }
}

impl FromStr for JsonPointer {
    type Err = CatalogModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl Display for JsonPointer {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        for token in &self.tokens {
            formatter.write_char('/')?;
            for character in token.as_str().chars() {
                match character {
                    '~' => formatter.write_str("~0")?,
                    '/' => formatter.write_str("~1")?,
                    value => formatter.write_char(value)?,
                }
            }
        }
        Ok(())
    }
}

fn decode_token(encoded: &str, token_index: usize) -> Result<JsonPointerToken, CatalogModelError> {
    let mut decoded = String::with_capacity(encoded.len());
    let mut characters = encoded.char_indices();
    while let Some((byte_offset, character)) = characters.next() {
        if character != '~' {
            decoded.push(character);
            continue;
        }
        match characters.next() {
            Some((_, '0')) => decoded.push('~'),
            Some((_, '1')) => decoded.push('/'),
            Some(_) | None => {
                return Err(CatalogModelError::InvalidJsonPointerEscape {
                    token_index,
                    byte_offset,
                });
            }
        }
    }
    Ok(JsonPointerToken::new(decoded))
}
