use async_trait::async_trait;

use crate::{prelude::BlockNumber, schema::InputSchema};

/// This is only needed to support the explorer API.
#[derive(Debug)]
pub struct VersionInfo {
    pub created_at: String,
    pub deployment_id: String,
    pub latest_ethereum_block_number: Option<BlockNumber>,
    pub total_ethereum_blocks_count: Option<BlockNumber>,
    pub synced: bool,
    pub failed: bool,
    pub description: Option<String>,
    pub repository: Option<String>,
    pub schema: InputSchema,
    pub network: String,
}

/// Common trait for index node server implementations.
#[async_trait]
pub trait IndexNodeServer {
    type ServeError;

    /// Creates a new Tokio task that, when spawned, brings up the index node server.
    async fn serve(&mut self, port: u16) -> Result<Result<(), ()>, Self::ServeError>;
}
