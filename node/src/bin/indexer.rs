use std::sync::Arc;

use graph::{
    anyhow::Result,
    blockchain::{block_stream::BlockStreamEvent, mock::MockBlockchain},
    indexer::{
        block_stream::IndexerBlockStream,
        store::{SledIndexerStore, DB_NAME},
    },
    prelude::{DeploymentHash, MetricsRegistry},
    slog::Logger,
    tokio,
    tokio_stream::StreamExt,
};
use uniswap::transform::Events;

#[tokio::main]
pub async fn main() -> Result<()> {
    let deployment: &str = "QmSB1Vw3ZmNX7wwkbPoybK944fDKzLZ3KWLhjbeD9DwyVL";
    let hash = DeploymentHash::new(deployment.clone()).unwrap();
    let db = Arc::new(sled::open(DB_NAME).unwrap());
    let store = Arc::new(
        SledIndexerStore::new(
            db,
            deployment,
            graph::indexer::store::StateSnapshotFrequency::Never,
        )
        .unwrap(),
    );
    let logger = graph::log::logger(true);
    let metrics = Arc::new(MetricsRegistry::mock());

    let mut stream = IndexerBlockStream::<MockBlockchain>::new(
        hash,
        store,
        None,
        vec![],
        vec![],
        logger,
        metrics,
    );
    loop {
        let x = stream.next().await.unwrap().unwrap();
        let (ptr, data) = match x {
            BlockStreamEvent::ProcessWasmBlock(ptr, _, data, _, _) => (ptr, data),
            _ => unreachable!(),
        };

        println!("====== {} ======", ptr.number);

        let evts: Events = borsh::from_slice(data.as_ref()).unwrap();

        println!("================");
    }

    Ok(())
}
