use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectStore;

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

    pub(crate) fn url(&self) -> &ObjectStoreUrl {
        &self.url
    }

    pub(crate) fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }
}
