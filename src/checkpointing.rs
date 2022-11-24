use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use itertools::Itertools;
use object_store::DynObjectStore;
use rocksdb::{DB, Options};
use rocksdb::checkpoint::Checkpoint;

const SST_FILES_SUBDIR: &str = "SSTs";
const LOG_FILES_SUBDIR: &str = "logs";
const LOG_FILES_LOCAL_SUBDIR: &str = "archve";

pub struct Checkpoints {
    checkpoint_location: String,
    local_location: String,
    storage: Arc<DynObjectStore>,
    pub(crate) db: DB,
}

impl Checkpoints {
    pub fn new(local_location: String, checkpoint_location: String, storage: Arc<DynObjectStore>) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        Self::with_options(local_location, checkpoint_location, storage, opts)
    }

    pub fn with_options(local_location: String, checkpoint_location: String, storage: Arc<DynObjectStore>, options: Options) -> Result<Self> {
        let db = DB::open(&options, &local_location)?;

        Ok(Self {
            checkpoint_location,
            local_location,
            storage,
            db,
        })
    }

    pub fn create_checkpoint(&self) -> Result<()> {
        let local_chkpnt_path = PathBuf::from(&self.checkpoint_location).join("local_checkpoints");

        Checkpoint::new(&self.db)?
            .create_checkpoint(local_chkpnt_path)
            .map_err(Into::into)
    }

    fn list_files(&self, path: PathBuf) -> Result<(Vec<PathBuf>, Vec<PathBuf>)> {
        let mut entries = vec![];
        for e in path.read_dir()? {
            let e = e?;
            let md = e.metadata()?;
            if md.is_dir() {
                entries.push(e.path());
            }
        }
        Ok(entries.into_iter()
            .partition(|f| {
                f.extension().map(|f| f == ".sts").unwrap_or(false)
            }))
    }
}

#[cfg(test)]
pub mod test {
    use object_store::local::LocalFileSystem;

    use super::*;

    #[test]
    pub fn test_checkpointing() -> Result<()> {
        let chkpnts = Checkpoints::new("test_db".to_string(), "test_db".to_string(), Arc::new(LocalFileSystem::new()))?;

        chkpnts.db.put("/mnt/test.txt", "asdf1234")?;
        chkpnts.db.put("/mnt/test2.txt", "1234asdf")?;

        chkpnts.create_checkpoint()
    }
}
