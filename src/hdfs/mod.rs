use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::stream::BoxStream;
use futures::StreamExt;
use hdfs::hdfs::{HdfsErr, HdfsFs};
use object_store::{GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore};
use object_store::path::Path;
use object_store::Result;
use tokio::io::{AsyncWrite, BufStream};

use crate::hdfs::file_stream::HdfsFileSteam;

mod file_stream;
mod utils;

fn map_error(err: HdfsErr) -> object_store::Error {
    match err {
        e @ HdfsErr::Generic(_) => object_store::Error::Generic { store: "", source: Box::new(e) },
        e @ HdfsErr::FileNotFound(_) => object_store::Error::NotFound { path: "".parse().unwrap(), source: Box::new(e) },
        e @ HdfsErr::FileAlreadyExists(_) => object_store::Error::AlreadyExists { path: "path.clone()".parse().unwrap(), source: Box::new(e) },
        e @ HdfsErr::CannotConnectToNameNode(_) => object_store::Error::Generic { store: "", source: Box::new(e) },
        e @ HdfsErr::InvalidUrl(_) => object_store::Error::Generic { store: "", source: Box::new(e) },
    }
}

#[derive(Debug)]
pub struct HdfsObjectStore {
    fs: Arc<HdfsFs>,
}

impl Display for HdfsObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[async_trait::async_trait]
impl ObjectStore for HdfsObjectStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let f = self.fs.append(&location.to_string()).map_err(map_error)?;
        f.write(&bytes).map_err(map_error)?;
        Ok(())
    }

    async fn put_multipart(&self, _location: &Path) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        unimplemented!()
    }

    async fn abort_multipart(&self, _location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        unimplemented!()
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let f = self.fs.open(&location.to_string()).map_err(map_error)?;
        let rs = HdfsFileSteam::new(f);
        Ok(GetResult::Stream(Box::pin(rs)))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let f = self.fs.open(&location.to_string()).map_err(map_error)?;
        let mut byte_buf = BytesMut::with_capacity(range.len());
        f.read_with_pos(range.start as i64, &mut byte_buf).map_err(map_error)?;

        Ok(byte_buf.freeze())
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let file_status = self.fs.get_file_status(location.as_ref()).map_err(map_error)?;
        let native_dt = NaiveDateTime::from_timestamp_millis(file_status.last_modified()).unwrap();
        Ok(
            ObjectMeta {
                location: location.clone(),
                last_modified: DateTime::from_utc(native_dt, Utc),
                size: file_status.len(),
            }
        )
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.fs.delete(location.as_ref(), false).map_err(map_error)?;
        Ok(())
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let path = prefix.cloned().unwrap_or(Path::from("/"));
        let files: Vec<Result<ObjectMeta>> = self.fs.list_status(path.as_ref()).map_err(map_error)?.into_iter().map(|file_status| {
            let dt = NaiveDateTime::from_timestamp_millis(file_status.last_modified()).unwrap_or(NaiveDateTime::MIN);
            Ok(ObjectMeta {
                location: path.clone(),
                last_modified: DateTime::from_utc(dt, Utc),
                size: file_status.len(),
            })
        }).collect();
        Ok(tokio_stream::iter(files.into_iter()).boxed())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        todo!()
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let dest = self.fs.create_with_overwrite(to.as_ref(), true).map_err(map_error)?;
        let source = self.fs.open(from.as_ref()).map_err(map_error)?;

        let mut buf_dest = BufStream::new(HdfsFileSteam::new(dest));
        let mut buf_source = BufStream::new(HdfsFileSteam::new(source));
        tokio::io::copy(&mut buf_source, &mut buf_dest).await.map_err(|err| {
            object_store::Error::Generic { store: "", source: Box::new(err) }
        })?;
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        todo!()
    }
}
