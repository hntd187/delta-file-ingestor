use serde::Deserialize;

#[derive(Deserialize)]
pub struct RestoreEventData {
    pub lifecycle_restoration_expiry_time: String,
    pub lifecycle_restore_storage_class: String,
}

#[derive(Deserialize)]
pub struct GlacierEventData {
    pub restore_event_data: RestoreEventData,
}

#[derive(Deserialize)]
pub struct Object {
    pub key: String,
    pub size: String,
    pub e_tag: String,
    pub version_id: String,
    pub sequencer: String,
}

#[derive(Deserialize)]
pub struct Bucket {
    pub name: String,
    pub owner_identity: UserId,
    pub arn: String,
}

#[derive(Deserialize)]
pub struct S3 {
    pub s3schema_version: String,
    pub configuration_id: String,
    pub bucket: Bucket,
    pub object: Object,
}

#[derive(Deserialize)]
pub struct ResponseElements {
    pub x_amz_request_id: String,
    pub x_amz_id_2: String,
}

#[derive(Deserialize)]
pub struct RequestParameters {
    pub source_ipaddress: String,
}

#[derive(Deserialize)]
pub struct UserId {
    pub principal_id: String,
}

#[derive(Deserialize)]
pub struct Event {
    pub event_version: String,
    pub event_source: String,
    pub aws_region: String,
    pub event_time: String,
    pub event_name: String,
    pub user_identity: UserId,
    pub request_parameters: RequestParameters,
    pub response_elements: ResponseElements,
    pub s3: S3,
    pub glacier_event_data: GlacierEventData,
}

#[derive(Deserialize)]
pub struct SqsEvent {
    pub records: Vec<Event>,
}
