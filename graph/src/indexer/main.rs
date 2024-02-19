use std::sync::Arc;

use graph::tokio;
use graph_core::indexer::store::{SledStoreError, DB_NAME};

#[tokio::main]
async fn main() {
    let db = Arc::new(sled::open(DB_NAME).map_err(SledStoreError::from).unwrap());

    println!("Hello World")
}
