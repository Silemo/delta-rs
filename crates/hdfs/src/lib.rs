use std::sync::Arc;
use log::debug;

use deltalake_core::logstore::{default_logstore, logstores, LogStore, LogStoreFactory};
use deltalake_core::storage::{
    factories, url_prefix_handler, ObjectStoreFactory, ObjectStoreRef, StorageOptions,
};
use deltalake_core::{DeltaResult, Path};
use hdfs_native_object_store::HdfsObjectStore;
use url::Url;

#[derive(Clone, Default, Debug)]
pub struct HdfsFactory {}

impl ObjectStoreFactory for HdfsFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        debug!("DBG - deltalake-hdfs : parse_url_opts \n");
        let store: ObjectStoreRef = Arc::new(HdfsObjectStore::with_config(
            url.as_str(),
            options.0.clone(),
        )?);
        let prefix = Path::parse(url.path())?;
        Ok((url_prefix_handler(store, prefix.clone()), prefix))
    }
}

impl LogStoreFactory for HdfsFactory {
    fn with_options(
        &self,
        store: ObjectStoreRef,
        location: &Url,
        options: &StorageOptions,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        debug!("DBG - deltalake-hdfs : with_options \n");
        Ok(default_logstore(store, location, options))
    }
}

/// Register an [ObjectStoreFactory] for common HDFS [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    debug!("DBG - deltalake-hdfs : register_handlers \n");
    let factory = Arc::new(HdfsFactory {});
    for scheme in ["hdfs", "viewfs"].iter() {
        let url = Url::parse(&format!("{}://", scheme)).unwrap();
        factories().insert(url.clone(), factory.clone());
        logstores().insert(url.clone(), factory.clone());
    }
}
