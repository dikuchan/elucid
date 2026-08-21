use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectStore;

use crate::EngineError;

#[derive(Clone, Debug)]
pub struct QueryObjectStore {
    url: ObjectStoreUrl,
    store: Arc<dyn ObjectStore>,
}

impl QueryObjectStore {
    #[must_use]
    pub fn new(url: ObjectStoreUrl, store: Arc<dyn ObjectStore>) -> Self {
        Self { url, store }
    }

    /// Creates the DataFusion object-store binding without exposing DataFusion URL types to the
    /// service boundary.
    ///
    /// # Errors
    ///
    /// Returns a stable execution error when the logical object-store URL is invalid.
    pub fn from_url(
        url: impl AsRef<str>,
        store: Arc<dyn ObjectStore>,
    ) -> Result<Self, EngineError> {
        ObjectStoreUrl::parse(url)
            .map(|url| Self::new(url, store))
            .map_err(EngineError::execution)
    }

    pub(crate) fn url(&self) -> &ObjectStoreUrl {
        &self.url
    }

    pub(crate) fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }
}
