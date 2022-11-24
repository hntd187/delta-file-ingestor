use std::collections::HashMap;

use anyhow::Result;
use deltalake::action::Protocol;
use deltalake::{
    DeltaTable, DeltaTableBuilder, DeltaTableMetaData, Schema, SchemaDataType, SchemaField,
};
use object_store::path::Path;

use crate::FileEvents;

pub struct StaticFileEvents(pub Vec<Path>);

#[async_trait::async_trait]
impl FileEvents for StaticFileEvents {
    async fn next_file(&mut self) -> Result<Vec<Path>> {
        Ok(self.0.clone())
    }
}

pub fn create_bare_table() -> DeltaTable {
    let table_dir = tempfile::tempdir().unwrap();
    let table_path = table_dir.path();
    dbg!(table_path);
    DeltaTableBuilder::from_uri(table_path.to_str().unwrap())
        .build()
        .unwrap()
}

pub fn test_type(name: &str, tpe: &str) -> SchemaField {
    SchemaField::new(
        name.to_string(),
        SchemaDataType::primitive(tpe.to_string()),
        true,
        HashMap::new(),
    )
}

pub async fn create_initialized_table(partition_cols: &[String]) -> DeltaTable {
    let mut table = create_bare_table();
    let table_schema = Schema::new(vec![
        test_type("id", "integer"),
        test_type("bool_col", "boolean"),
        test_type("tinyint_col", "integer"),
        test_type("smallint_col", "integer"),
        test_type("int_col", "integer"),
        test_type("bigint_col", "long"),
        test_type("float_col", "float"),
        test_type("double_col", "double"),
        test_type("date_string_col", "binary"),
        test_type("string_col", "binary"),
    ]);

    let mut commit_info = serde_json::Map::<String, serde_json::Value>::new();
    commit_info.insert(
        "operation".to_string(),
        serde_json::Value::String("CREATE TABLE".to_string()),
    );
    commit_info.insert(
        "userName".to_string(),
        serde_json::Value::String("test user".to_string()),
    );
    let protocol = Protocol {
        min_reader_version: 1,
        min_writer_version: 1,
    };
    let metadata = DeltaTableMetaData::new(
        None,
        None,
        None,
        table_schema,
        partition_cols.to_vec(),
        HashMap::new(),
    );

    table
        .create(metadata, protocol, Some(commit_info), None)
        .await
        .unwrap();

    table
}
