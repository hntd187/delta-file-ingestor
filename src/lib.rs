#![feature(async_fn_in_trait)]

use anyhow::Result;
use object_store::path::Path;

pub mod aws;
pub mod local;
pub mod processor;
pub mod uc;

#[cfg(test)]
mod test_utils;

pub trait FileEvents {
    async fn next_file(&mut self) -> Result<Vec<Path>>;
}
