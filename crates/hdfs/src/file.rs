use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    ObjectStore, PutPayload, PutMultipartOpts,
    path::Path as ObjectStorePath, GetOptions, GetResult, ListResult,
    MultipartUpload, ObjectMeta, PutOptions, PutResult, Result as ObjectStoreResult
};
use object_store_hdfs::HadoopFileSystem;
use std::ops::Range;
use std::sync::Arc;
use url::Url;

pub(crate) const STORE_NAME: &str = "HdfsObjectStore";

/// Hadoop File Storage Backend.
#[derive(Debug)]
pub struct HadoopFileStorageBackend {
    inner: Arc <HadoopFileSystem>,
    root_url: Arc <Url>,
}

impl HadoopFileStorageBackend {
    /// Creates a new HadoopFileStorageBackend.
    pub fn try_new(path: &str) -> ObjectStoreResult<Self> {
        Ok(Self {
            root_url: Arc::new(Self::path_to_root_url(path.as_ref())?),
            inner: Arc::new(HadoopFileSystem::new_from_full_path(path).unwrap()),
        })
    }

    fn path_to_root_url(path: &std::path::Path) -> ObjectStoreResult<Url> {
        let root_path =
            std::fs::canonicalize(path).map_err(|e| object_store::Error::InvalidPath {
                source: object_store::path::Error::Canonicalize {
                    path: path.into(),
                    source: e,
                },
            })?;

        Url::from_file_path(root_path).map_err(|_| object_store::Error::InvalidPath {
            source: object_store::path::Error::InvalidPath { path: path.into() },
        })
    }

    // TODO Remove code if not useful
    /// Return an absolute filesystem path of the given location
    fn path_to_filesystem(&self, location: &ObjectStorePath) -> String {
        let mut url = self.root_url.as_ref().clone();
        url.path_segments_mut()
            .expect("url path")
            // technically not necessary as Path ignores empty segments
            // but avoids creating paths with "//" which look odd in error messages.
            .pop_if_empty()
            .extend(location.parts());

        url.to_file_path().unwrap().to_str().unwrap().to_owned()
    }
}

impl std::fmt::Display for HadoopFileStorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HadoopFileStorageBackend")
    }
}

#[async_trait::async_trait]
impl ObjectStore for HadoopFileStorageBackend {
    async fn put(&self, location: &ObjectStorePath, bytes: PutPayload) -> ObjectStoreResult<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &ObjectStorePath,
        bytes: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, bytes, options).await
    }

    async fn get(&self, location: &ObjectStorePath) -> ObjectStoreResult<GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(
        &self,
        location: &ObjectStorePath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(
        &self,
        location: &ObjectStorePath,
        range: Range<usize>,
    ) -> ObjectStoreResult<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &ObjectStorePath) -> ObjectStoreResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &ObjectStorePath) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    //fn list_with_offset(
    //    &self,
    //    prefix: Option<&ObjectStorePath>,
    //    offset: &ObjectStorePath,
    //) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
    //    self.inner.list_with_offset(prefix, offset)
    //}

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectStorePath, to: &ObjectStorePath) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> ObjectStoreResult<()> {
        // Is path_to_filesystem necessary(?)
        //let path_from = self.path_to_filesystem(from);
        //let path_to = self.path_to_filesystem(to);
        self.inner.rename(from, to).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectStorePath,
        opts: PutMultipartOpts
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        todo!("Not yet implemented")
    }
}
