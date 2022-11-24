use deltalake::{Schema, SchemaField};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Properties {
    #[serde(rename = "delta.lastCommitTimestamp")]
    pub delta_last_commit_timestamp: String,
    #[serde(rename = "delta.lastUpdateVersion")]
    pub delta_last_update_version: String,
    #[serde(rename = "delta.minReaderVersion")]
    pub delta_min_reader_version: String,
    #[serde(rename = "delta.minWriterVersion")]
    pub delta_min_writer_version: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Column {
    pub name: String,
    pub type_text: String,
    pub type_json: String,
    pub type_name: String,
    pub type_precision: i64,
    pub type_scale: i64,
    pub position: i64,
    pub nullable: bool,
    pub partition_index: Option<i16>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UnityCatalogSchema {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub table_type: String,
    pub data_source_format: String,
    pub columns: Vec<Column>,
    pub storage_location: String,
    pub owner: String,
    pub properties: Properties,
    pub generation: i64,
    pub metastore_id: String,
    pub full_name: String,
    pub data_access_configuration_id: String,
    pub created_at: i64,
    pub created_by: String,
    pub updated_at: i64,
    pub updated_by: String,
    pub table_id: String,
}

impl Into<Schema> for UnityCatalogSchema {
    fn into(self) -> Schema {
        let fields = self
            .columns
            .into_iter()
            .flat_map(|col| serde_json::from_str::<SchemaField>(&col.type_json))
            .collect::<Vec<_>>();

        Schema::new(fields)
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use std::fs::File;
    use std::io::BufReader;

    #[test]
    pub fn test_deser() -> anyhow::Result<()> {
        let mut reader = BufReader::new(File::open("./test_files/uc_schema.json")?);
        let schema: UnityCatalogSchema = serde_json::from_reader(&mut reader)?;
        let delta_schema: Schema = schema.into();
        dbg!(delta_schema);
        Ok(())
    }
}
