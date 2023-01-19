use reqwest::{Client, Url};

use super::*;

#[derive(Debug, Clone)]
pub struct UnityCatalogClient {
    opts: UnityCatalogOptions,
    api_client: Client,
    endpoint: Url,
}

impl UnityCatalogClient {
    pub fn new(opts: UnityCatalogOptions) -> Result<Self> {
        let api_client = Client::builder().deflate(true).build()?;
        let endpoint = Url::parse(&opts.db_api_host)?.join(API_PATH)?;
        Ok(Self {
            api_client,
            opts,
            endpoint,
        })
    }
}

impl UnityCatalogApi for UnityCatalogClient {
    async fn get_table_schema<T>(&self, table: T) -> Result<UnityCatalogSchema>
        where T: fmt::Display {

        let full_name = format!(
            "{}.{}.{}",
            self.opts.default_catalog, self.opts.default_schema, table
        );
        let endpoint = self.endpoint.join(&full_name)?;

        self.api_client
            .get(endpoint)
            .bearer_auth(&self.opts.db_api_token)
            .send()
            .await?
            .json::<UnityCatalogSchema>()
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
pub mod test {
    use crate::uc::client::UnityCatalogClient;
    use crate::uc::UnityCatalogOptions;

    #[test]
    pub fn test_urls() {
        let mut opts = UnityCatalogOptions::default();
        opts.db_api_host = String::from("https://demo.cloud.databricks.com/");
        let client = UnityCatalogClient::new(opts);
        dbg!(client).expect("TODO: panic message");
    }
}
