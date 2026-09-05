use std::path::PathBuf;

use elucid_core::ErrorCode;
use reqwest::Body;
use tokio::io::BufReader;
use tokio_util::io::ReaderStream;

use crate::error::Failure;

#[derive(Debug)]
pub(crate) enum RequestInput {
    File(PathBuf),
    StandardInput,
}

impl RequestInput {
    pub(crate) fn from_path_or_dash(path: PathBuf) -> Self {
        if path.as_os_str() == "-" {
            Self::StandardInput
        } else {
            Self::File(path)
        }
    }

    pub(crate) async fn into_body(self) -> Result<Body, Failure> {
        match self {
            Self::File(path) => {
                let file = tokio::fs::File::open(&path).await.map_err(|error| {
                    Failure::command(
                        ErrorCode::InputFileUnreadable,
                        anyhow::Error::new(error)
                            .context(format!("failed to open input file {path:?}")),
                    )
                })?;
                let stream = ReaderStream::new(BufReader::new(file));
                Ok(Body::wrap_stream(stream))
            }
            Self::StandardInput => {
                let stream = ReaderStream::new(BufReader::new(tokio::io::stdin()));
                Ok(Body::wrap_stream(stream))
            }
        }
    }
}
