use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncBufReadExt;

use crate::event::{EventContext, RawEvent};
use crate::stage_error::StageError;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct LineSourceEventContext {
    pub line_no: u64,
    pub file_path: Option<std::path::PathBuf>,
}

impl EventContext for LineSourceEventContext {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "line_no": self.line_no,
            "file_path": self.file_path.as_ref().map(|p| p.to_string_lossy().into_owned()),
        })
    }
}

pub struct LineSource<R> {
    inner: tokio_stream::wrappers::LinesStream<tokio::io::BufReader<R>>,
    line_no: u64,
    max_line_byte_count: usize,
}

impl<R: tokio::io::AsyncBufRead> LineSource<R> {
    pub fn new(reader: R, max_line_byte_count: usize) -> Self {
        Self {
            inner: tokio_stream::wrappers::LinesStream::new(
                tokio::io::BufReader::new(reader).lines(),
            ),
            line_no: 0,
            max_line_byte_count,
        }
    }
}

impl<R: tokio::io::AsyncBufRead + Unpin> futures::Stream for LineSource<R> {
    type Item = Result<RawEvent<LineSourceEventContext>, StageError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Ok(line))) => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    self.line_no += 1;
                    if line.len() > self.max_line_byte_count {
                        return Poll::Ready(Some(Err(StageError::LineTooLarge {
                            size: line.len(),
                            max: self.max_line_byte_count,
                        })));
                    }
                    return Poll::Ready(Some(Ok(RawEvent {
                        raw: line.to_owned(),
                        context: LineSourceEventContext {
                            line_no: self.line_no,
                            file_path: None,
                        },
                    })));
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(StageError::Wal(e))));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::StreamExt;

    fn stream_from_str(s: &str, max: usize) -> LineSource<&[u8]> {
        LineSource::new(s.as_bytes(), max)
    }

    #[tokio::test]
    async fn three_lines_three_events() {
        let events: Vec<_> = stream_from_str("line1\nline2\nline3\n", 1024)
            .collect()
            .await;
        let events: Vec<_> = events.into_iter().filter_map(|r| r.ok()).collect();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].raw, "line1");
        assert_eq!(events[0].context.line_no, 1);
        assert_eq!(events[1].raw, "line2");
        assert_eq!(events[1].context.line_no, 2);
        assert_eq!(events[2].raw, "line3");
        assert_eq!(events[2].context.line_no, 3);
    }

    #[tokio::test]
    async fn blank_lines_skipped() {
        let events: Vec<_> = stream_from_str("line1\n\n\nline2\n", 1024).collect().await;
        let events: Vec<_> = events.into_iter().filter_map(|r| r.ok()).collect();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].context.line_no, 1);
        assert_eq!(events[1].context.line_no, 2);
    }

    #[tokio::test]
    async fn oversized_line_error_then_continue() {
        let events: Vec<_> = stream_from_str("ok\nTHIS_IS_A_VERY_LONG_LINE\nalso_ok\n", 10)
            .collect()
            .await;
        assert_eq!(events.len(), 3);
        assert!(events[0].is_ok());
        assert!(events[1].is_err());
        assert!(events[2].is_ok());
        assert_eq!(events[2].as_ref().unwrap().raw, "also_ok");
    }

    #[tokio::test]
    async fn empty_input() {
        let events: Vec<_> = stream_from_str("", 1024).collect().await;
        assert!(events.is_empty());
    }
}
