use std::fmt;
use anyhow::Result;
pub use client::UnityCatalogClient;
pub use model::*;

mod client;
mod model;

const API_PATH: &str = "/api/2.1/unity-catalog/tables/";

#[derive(Debug, Clone, Default)]
pub struct UnityCatalogOptions {
    pub db_api_host: String,
    pub db_api_token: String,
    pub default_catalog: String,
    pub default_schema: String,
}

pub trait UnityCatalogApi {
    async fn get_table_schema<T>(&self, table: T) -> Result<UnityCatalogSchema>
        where T: fmt::Display;
}

