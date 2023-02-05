use std::io::Error;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::Stream;
use hdfs::hdfs::HdfsFile;
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf, SeekFrom};

use super::map_error;

pin_project! {
    pub struct HdfsFileSteam {
        #[pin]
        reader: Arc<HdfsFile>,
    }
}

impl HdfsFileSteam {
    pub fn new(reader: HdfsFile) -> Self {
        Self { reader: Arc::new(reader) }
    }
}

impl AsyncRead for HdfsFileSteam {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let read_len = self.reader.read(buf.initialize_unfilled()).map_err(map_error)?;
        if read_len > 0 {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl AsyncSeek for HdfsFileSteam {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        match position {
            SeekFrom::Start(pos) => {
                self.reader.seek(pos);
            }
            SeekFrom::End(pos) => {
                let size = self.reader.get_file_status().map_err(map_error)?.len();
                let new_pos = size as u64 + pos as u64;
                self.reader.seek(new_pos);
            }
            SeekFrom::Current(pos) => {
                let current_pos = self.reader.pos().map_err(map_error)?;
                self.reader.seek(current_pos + pos as u64);
            }
        }
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        let pos = self.reader.pos().map_err(map_error)?;
        Poll::Ready(Ok(pos))
    }
}

impl AsyncWrite for HdfsFileSteam {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<StdResult<usize, Error>> {
        let bytes_written = self.reader.write(buf).map_err(map_error)?;

        Poll::Ready(Ok(bytes_written as usize))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<StdResult<(), Error>> {
        self.reader.flush();
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<StdResult<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

impl Stream for HdfsFileSteam {
    type Item = StdResult<Bytes, object_store::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut buf = BytesMut::new();
        let mut read_buf = ReadBuf::new(&mut buf);
        match self.poll_read(cx, &mut read_buf) {
            Poll::Ready(_) => {
                if buf.len() > 0 {
                    Poll::Ready(Some(Ok(buf.freeze())))
                } else {
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending
        }
    }
}
