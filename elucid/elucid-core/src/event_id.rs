use std::array::TryFromSliceError;
use std::fmt::{self, Debug, Display, Formatter};
use std::str::FromStr;

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct EventId([u8; 16]);

impl EventId {
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl From<[u8; 16]> for EventId {
    fn from(value: [u8; 16]) -> Self {
        Self(value)
    }
}

impl TryFrom<&[u8]> for EventId {
    type Error = TryFromSliceError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        value.try_into().map(Self)
    }
}

impl FromStr for EventId {
    type Err = ParseEventIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 32 {
            return Err(ParseEventIdError);
        }
        let mut bytes = [0_u8; 16];
        for (output, pair) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
            *output = (hex_value(pair[0])? << 4) | hex_value(pair[1])?;
        }
        Ok(Self(bytes))
    }
}

impl Display for EventId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = [0_u8; 32];
        for (pair, byte) in encoded.chunks_exact_mut(2).zip(self.0) {
            pair[0] = HEX[usize::from(byte >> 4)];
            pair[1] = HEX[usize::from(byte & 0x0f)];
        }
        formatter.write_str(std::str::from_utf8(&encoded).map_err(|_| fmt::Error)?)
    }
}

impl Debug for EventId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0, formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("event ID must contain exactly 32 lowercase hexadecimal characters")]
#[non_exhaustive]
pub struct ParseEventIdError;

fn hex_value(value: u8) -> Result<u8, ParseEventIdError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(ParseEventIdError),
    }
}
