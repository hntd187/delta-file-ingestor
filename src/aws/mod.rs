pub mod model;
pub mod sqs;

#[derive(Debug, Clone, Default)]
pub struct SqsEventOptions {
    pub queue_name: String,
}
