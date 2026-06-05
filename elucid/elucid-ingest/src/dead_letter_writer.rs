use std::fmt::{self, Debug, Formatter};
use std::io;

use tokio::io::AsyncWriteExt;

use crate::event::EventContext;
use crate::stage_error::StageError;

pub struct DeadLetterWriter<W> {
    inner: W,
    count: u64,
}

impl<W: Debug> Debug for DeadLetterWriter<W> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeadLetterWriter")
            .field("inner", &self.inner)
            .field("count", &self.count)
            .finish()
    }
}

impl<W> DeadLetterWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner, count: 0 }
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn inner(&self) -> &W {
        &self.inner
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: tokio::io::AsyncWrite + Unpin> DeadLetterWriter<W> {
    pub async fn write<C: EventContext>(
        &mut self,
        raw: &str,
        error: &StageError,
        context: &C,
    ) -> io::Result<()> {
        let value = serde_json::json!({
            "@message": raw,
            "@error": error.to_string(),
            "@context": context.to_json(),
        });
        let mut line = serde_json::to_string(&value).map_err(|e| io::Error::other(e))?;
        line.push('\n');
        self.inner.write_all(line.as_bytes()).await?;
        self.count += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::pin::Pin;
    use std::task::{Context, Poll};

    use crate::line_source::LineSourceEventContext;

    struct TestWriter(Vec<u8>);

    impl TestWriter {
        fn new() -> Self {
            Self(Vec::new())
        }

        fn into_bytes(self) -> Vec<u8> {
            self.0
        }
    }

    impl tokio::io::AsyncWrite for TestWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.get_mut().0.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn parse_entries(data: &[u8]) -> Vec<serde_json::Value> {
        let s = String::from_utf8(data.to_vec()).expect("valid UTF-8");
        s.lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).expect("valid JSON"))
            .collect()
    }

    #[tokio::test]
    async fn write_single_entry_has_required_fields() {
        let mut writer = DeadLetterWriter::new(TestWriter::new());
        let ctx = LineSourceEventContext {
            line_no: 1,
            file_path: None,
        };
        let error = StageError::Parse("bad token".to_owned());
        writer
            .write(r#"{"a":1}"#, &error, &ctx)
            .await
            .expect("write");

        let data = writer.into_inner().into_bytes();
        let entries = parse_entries(&data);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["@message"], r#"{"a":1}"#);
        assert_eq!(entries[0]["@error"], "JSON parse error: bad token");
        assert!(entries[0]["@context"]["line_no"].is_number());
    }

    #[tokio::test]
    async fn write_three_entries_count_and_lines() {
        let mut writer = DeadLetterWriter::new(TestWriter::new());
        let ctx = LineSourceEventContext {
            line_no: 1,
            file_path: None,
        };
        writer
            .write("line1", &StageError::Parse("e1".to_owned()), &ctx)
            .await
            .expect("write 1");
        writer
            .write("line2", &StageError::Parse("e2".to_owned()), &ctx)
            .await
            .expect("write 2");
        writer
            .write("line3", &StageError::Parse("e3".to_owned()), &ctx)
            .await
            .expect("write 3");

        assert_eq!(writer.count(), 3);

        let data = writer.into_inner().into_bytes();
        let entries = parse_entries(&data);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0]["@message"], "line1");
        assert_eq!(entries[1]["@message"], "line2");
        assert_eq!(entries[2]["@message"], "line3");
    }

    #[tokio::test]
    async fn write_context_contains_line_number() {
        let mut writer = DeadLetterWriter::new(TestWriter::new());
        let ctx = LineSourceEventContext {
            line_no: 99,
            file_path: None,
        };
        writer
            .write("raw", &StageError::Parse("err".to_owned()), &ctx)
            .await
            .expect("write");

        let data = writer.into_inner().into_bytes();
        let entries = parse_entries(&data);
        assert_eq!(entries[0]["@context"]["line_no"], 99);
    }

    #[tokio::test]
    async fn into_inner_returns_all_data() {
        let mut writer = DeadLetterWriter::new(TestWriter::new());
        let ctx = LineSourceEventContext {
            line_no: 1,
            file_path: None,
        };
        writer
            .write("first", &StageError::Parse("a".to_owned()), &ctx)
            .await
            .expect("write 1");
        writer
            .write("second", &StageError::Parse("b".to_owned()), &ctx)
            .await
            .expect("write 2");

        let inner = writer.into_inner();
        let data = inner.into_bytes();
        let s = String::from_utf8(data).expect("UTF-8");
        assert_eq!(s.lines().count(), 2);
    }

    #[tokio::test]
    async fn write_escapes_special_characters() {
        let mut writer = DeadLetterWriter::new(TestWriter::new());
        let ctx = LineSourceEventContext {
            line_no: 1,
            file_path: None,
        };
        let raw = r#"has "quotes" and	tab"#;
        let error = StageError::Normalization("missing field 'ts'".to_owned());
        writer.write(raw, &error, &ctx).await.expect("write");

        let data = writer.into_inner().into_bytes();
        let entries = parse_entries(&data);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["@message"], "has \"quotes\" and\ttab");
    }
}
