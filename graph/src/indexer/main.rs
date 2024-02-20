use std::sync::Arc;

use graph::components::store::DeploymentLocator;
use graph::indexer::store::{SledStoreError, DB_NAME};
use graph::tokio;

#[tokio::main]
async fn main() {
    let db = Arc::new(sled::open(DB_NAME).map_err(SledStoreError::from).unwrap());
    // let deployments = DeploymentLocator::new();

    println!("Hello World")
}
