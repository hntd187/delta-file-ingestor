use crate::Result;

pub async fn maybe_spawn_blocking<F, T>(f: F) -> Result<T, anyhow::Error>
    where
        F: FnOnce() -> Result<T> + Send + 'static,
        T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => runtime.spawn_blocking(f).await?,
        Err(_) => f(),
    }
}
