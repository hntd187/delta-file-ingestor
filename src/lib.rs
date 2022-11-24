extern crate core;

use anyhow::Result;
use object_store::path::Path;

pub mod aws;
pub mod checkpointing;
pub mod local;
pub mod processor;
pub mod uc;

#[cfg(test)]
mod test_utils;

#[async_trait::async_trait]
pub trait FileEvents {
    async fn next_file(&mut self) -> Result<Vec<Path>>;
}
