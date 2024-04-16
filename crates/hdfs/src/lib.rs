use std::sync::Arc;

use deltalake_core::logstore::{default_logstore, logstores, LogStore, LogStoreFactory};
use deltalake_core::storage::{
    factories, str_is_truthy, ObjectStoreFactory, ObjectStoreRef, StorageOptions,
};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use url::Url;

pub mod error;
mod file;

#[derive(Clone, Default, Debug)]
pub struct HdfsFactory {}

impl ObjectStoreFactory for HdfsFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {

        match url.scheme() {
            "hdfs" => {
                let store = Arc::new(file::HadoopFileStorageBackend::try_new(
                    url.to_file_path().unwrap().to_str().expect("Parsed hdfs url"),
                )?) as ObjectStoreRef;
                Ok((store, Path::from("/")))
            }
            _ => Err(DeltaTableError::InvalidTableLocation(url.clone().into())),
        }
    }
}

impl LogStoreFactory for HdfsFactory {
    fn with_options(
        &self,
        store: ObjectStoreRef,
        location: &Url,
        options: &StorageOptions,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(store, location, options))
    }
}

/// Register an [ObjectStoreFactory] for common Hdfs [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let factory = Arc::new(HdfsFactory {});
    for scheme in ["hdfs"].iter() {
        let url = Url::parse(&format!("{}://", scheme)).unwrap();
        factories().insert(url.clone(), factory.clone());
        logstores().insert(url.clone(), factory.clone());
    }
}
