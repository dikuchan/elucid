use std::fs::{File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use crate::{SpoolCheckpoint, SpoolError};

const CHECKPOINT_FILE_NAME: &str = "spool.checkpoint";
const CHECKPOINT_MAGIC: &[u8; 8] = b"ELUCKP01";
const CHECKPOINT_VERSION: u16 = 1;
const CHECKPOINT_PREFIX_BYTES: usize = 20;
const CHECKPOINT_BYTES: usize = 52;

pub(crate) fn create_new(directory: &Path) -> Result<(), SpoolError> {
    let path = path(directory);
    let bytes = encode(SpoolCheckpoint::INITIAL);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|source| SpoolError::io("create the spool checkpoint", source))?;
    file.write_all(&bytes)
        .map_err(|source| SpoolError::io("write the spool checkpoint", source))?;
    file.sync_all()
        .map_err(|source| SpoolError::io("synchronize the spool checkpoint", source))
}

pub(crate) fn load(directory: &Path) -> Result<SpoolCheckpoint, SpoolError> {
    let path = path(directory);
    let metadata = match std::fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Err(SpoolError::corrupt("spool checkpoint is missing"));
        }
        Err(source) => return Err(SpoolError::io("inspect the spool checkpoint", source)),
    };
    if metadata.len() != CHECKPOINT_BYTES as u64 {
        return Err(SpoolError::corrupt("spool checkpoint length is invalid"));
    }
    let mut bytes = [0_u8; CHECKPOINT_BYTES];
    File::open(path)
        .and_then(|mut file| file.read_exact(&mut bytes))
        .map_err(|source| SpoolError::io("read the spool checkpoint", source))?;
    decode(&bytes)
}

fn path(directory: &Path) -> PathBuf {
    directory.join(CHECKPOINT_FILE_NAME)
}

fn encode(checkpoint: SpoolCheckpoint) -> [u8; CHECKPOINT_BYTES] {
    let mut bytes = [0_u8; CHECKPOINT_BYTES];
    bytes[..8].copy_from_slice(CHECKPOINT_MAGIC);
    bytes[8..10].copy_from_slice(&CHECKPOINT_VERSION.to_be_bytes());
    bytes[10..12].copy_from_slice(&(CHECKPOINT_BYTES as u16).to_be_bytes());
    bytes[12..20].copy_from_slice(&checkpoint.position().to_be_bytes());
    let digest = blake3::hash(&bytes[..CHECKPOINT_PREFIX_BYTES]);
    bytes[CHECKPOINT_PREFIX_BYTES..].copy_from_slice(digest.as_bytes());
    bytes
}

fn decode(bytes: &[u8; CHECKPOINT_BYTES]) -> Result<SpoolCheckpoint, SpoolError> {
    if &bytes[..8] != CHECKPOINT_MAGIC {
        return Err(SpoolError::corrupt("spool checkpoint magic is invalid"));
    }
    if u16::from_be_bytes(copy_array(&bytes[8..10])?) != CHECKPOINT_VERSION {
        return Err(SpoolError::corrupt(
            "spool checkpoint version is unsupported",
        ));
    }
    if usize::from(u16::from_be_bytes(copy_array(&bytes[10..12])?)) != CHECKPOINT_BYTES {
        return Err(SpoolError::corrupt("spool checkpoint framing is invalid"));
    }
    let expected_digest = blake3::hash(&bytes[..CHECKPOINT_PREFIX_BYTES]);
    if &bytes[CHECKPOINT_PREFIX_BYTES..] != expected_digest.as_bytes() {
        return Err(SpoolError::corrupt(
            "spool checkpoint digest does not match",
        ));
    }
    Ok(SpoolCheckpoint::from_position(u64::from_be_bytes(
        copy_array(&bytes[12..20])?,
    )))
}

fn copy_array<const SIZE: usize>(bytes: &[u8]) -> Result<[u8; SIZE], SpoolError> {
    bytes
        .try_into()
        .map_err(|_| SpoolError::invariant("fixed-size checkpoint field has the wrong length"))
}
