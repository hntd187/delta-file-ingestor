use anyhow::Result;
use aws_sdk_sqs::Client;
use object_store::path::Path;

use crate::FileEvents;

use super::{model::*, *};

pub struct SqsEvents {
    client: Client,
    opts: SqsEventOptions,
}

impl SqsEvents {
    pub fn new(client: Client, opts: SqsEventOptions) -> Self {
        Self { client, opts }
    }
}

#[async_trait::async_trait]
impl FileEvents for SqsEvents {
    async fn next_file(&mut self) -> Result<Vec<Path>> {
        let msg_que = self.client.receive_message().send().await?;
        let msgs = msg_que.messages().unwrap_or_default();

        Ok(msgs
            .into_iter()
            .flat_map(|msg| msg.body())
            .flat_map(|msg| serde_json::from_str::<SqsEvent>(msg))
            .flat_map(|msg| {
                msg.records
                    .into_iter()
                    .map(|m| {
                        Path::parse(&format!("{}/{}", m.s3.bucket.name, m.s3.object.key)).unwrap()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>())
    }
}
