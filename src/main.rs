use clap::Parser;
use object_store::ObjectStore;
use tokio::time::interval;

use delta_file_ingest::aws::sqs::SqsEvents;
use delta_file_ingest::aws::SqsEventOptions;
use delta_file_ingest::FileEvents;
use delta_file_ingest::processor::{EventProcessor, EventProcessorOptions};
use delta_file_ingest::uc::{UnityCatalogApi, UnityCatalogClient, UnityCatalogOptions};

#[derive(Clone, Debug, Parser)]
#[command(author, version, about)]
pub struct RunOptions {
    #[arg(short, long)]
    table_name: String,
    // Event Processor Opts
    #[arg(long, default_missing_value = "10s")]
    poll_time: humantime::Duration,

    // UC Options
    #[arg(long)]
    db_api_host: String,
    #[arg(long)]
    db_api_token: String,
    #[arg(long, default_missing_value = "default")]
    default_catalog: String,
    #[arg(long)]
    default_schema: String,
    // Message Queue Options
    #[arg(long)]
    queue_name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let RunOptions {
        table_name,
        poll_time,
        db_api_host,
        db_api_token,
        default_catalog,
        default_schema,
        queue_name,
    } = RunOptions::parse();
    let queue_options = SqsEventOptions { queue_name };
    let uc_options = UnityCatalogOptions {
        db_api_host,
        default_catalog,
        db_api_token,
        default_schema,
    };
    let event_proc_options = EventProcessorOptions {
        poll_time: poll_time.as_secs(),
    };

    let uc = UnityCatalogClient::new(uc_options)?;
    let storage_location = uc.get_table_schema(table_name).await?.storage_location;

    let events = setup_events(queue_options).await;
    let storage = setup_storage();
    let table = deltalake::open_table(storage_location).await?;
    let mut event_processor = EventProcessor::new(events, storage, table, event_proc_options)?;

    loop {
        let _ = interval(poll_time.into()).tick().await;

        if let Err(_err) = event_processor.run().await {
            panic!("Failed to process event: {:?}", _err);
        }
    }
}

pub async fn setup_events(opts: SqsEventOptions) -> impl FileEvents {
    let shared_config = aws_config::load_from_env().await;
    let client = aws_sdk_sqs::Client::new(&shared_config);
    SqsEvents::new(client, opts)
}

pub fn setup_storage() -> impl ObjectStore {
    object_store::aws::AmazonS3Builder::from_env()
        .build()
        .expect("Unable to create S3 Client")
}
