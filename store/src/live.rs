//! Hides files a merge has already replaced, so a reader never counts their
//! rows twice.
//!
//! They last only until the deletes land — but a failed delete leaves them
//! indefinitely, and until then the merge and its sources are both readable.

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use datafusion::object_store::path::Path;
use datafusion::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use futures::stream::{BoxStream, StreamExt, TryStreamExt};

use crate::merged::split_live;

/// An [`ObjectStore`] whose listings omit files that a merged file in the same
/// listing has replaced.
#[derive(Debug)]
pub struct LiveStore(Arc<dyn ObjectStore>);

impl LiveStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self(inner)
    }
}

impl Display for LiveStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LiveStore({})", self.0)
    }
}

#[async_trait::async_trait]
impl ObjectStore for LiveStore {
    /// Collects before filtering: a merged file and the sources it replaced are
    /// written to one directory, so whether a source is stale can only be
    /// decided once the whole listing is in hand.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let listing = self.0.list(prefix);

        futures::stream::once(async move {
            let live: Vec<Result<ObjectMeta>> = match listing.try_collect().await {
                Ok(listing) => split_live(listing).0.into_iter().map(Ok).collect(),
                Err(error) => vec![Err(error)],
            };

            futures::stream::iter(live)
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let listed = self.0.list_with_delimiter(prefix).await?;

        Ok(ListResult {
            objects: split_live(listed.objects).0,
            common_prefixes: listed.common_prefixes,
        })
    }

    // Plain forwarding from here: the trait requires all of these, so there is
    // nothing to decide. The one deliberate omission is `list_with_offset`,
    // whose default filters what `list` returns.

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.0.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.0.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.0.delete_stream(locations)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.0.copy_opts(from, to, options).await
    }
}
