use std::{collections::HashMap, pin::Pin, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use borsh::BorshSerialize;
use futures::{Stream, StreamExt};
use graph::{
    blockchain::{
        block_stream::{BlockStreamEvent, FirehoseCursor},
        Blockchain,
    },
    components::store::{DeploymentCursorTracker, DeploymentLocator},
    data::subgraph::UnifiedMappingApiVersion,
    itertools::Itertools,
    prelude::BlockNumber,
};

pub mod store;
mod uniswap;

pub type Item = Box<[u8]>;

enum StateOperation {
    Set(Key, Item),
    Unset(Key, Item),
}

#[derive(Default)]
pub struct State {
    items: HashMap<Box<[u8]>, Item>,
    tags: HashMap<&'static str, Vec<Box<[u8]>>>,
    deltas: Vec<StateOperation>,
}

#[derive(Hash)]
pub struct Key {
    pub id: Box<[u8]>,
    pub tag: Option<&'static str>,
}

impl State {
    pub fn set_encode<B: BorshSerialize>(&mut self, key: Key, item: B) -> Result<()> {
        let mut buf = vec![];
        item.serialize(&mut buf)?;

        self.set(key, buf)
    }

    pub fn set(&mut self, key: Key, item: impl AsRef<[u8]>) -> Result<()> {
        unimplemented!();
    }
    pub fn get(&mut self, key: Key) -> Result<Option<Item>> {
        unimplemented!()
    }
    pub fn get_keys(&mut self, tag: &'static str) -> Result<Vec<Key>> {
        let keys = self
            .tags
            .get(tag)
            .unwrap_or(&vec![])
            .into_iter()
            .map(|k| Key {
                id: k.clone(),
                tag: Some(tag),
            })
            .collect_vec();

        Ok(keys)
    }
}

pub struct EncodedBlock(Box<[u8]>);
pub struct EncodedTrigger(Box<[u8]>);
pub struct TriggerMap(HashMap<BlockNumber, Vec<EncodedTrigger>>);

#[async_trait]
/// Indexer store is the store where the triggers will be kept to be processed by subgraphs
/// later. Only the latest state is kept and will be limited in size. State is not mandatory
/// and will not be queryable outside the pre-Indexing process.
pub trait IndexerStore {
    async fn get(&self, bn: BlockNumber) -> Result<Vec<EncodedTrigger>>;
    async fn set(
        &self,
        bn: BlockNumber,
        state: &State,
        triggers: Vec<EncodedTrigger>,
    ) -> Result<()>;
    async fn get_state(&self) -> Result<State>;
    async fn get_all(&self, tag: &str) -> Result<Vec<EncodedTrigger>>;
}

/// BlockTransform the specific transformation to apply to every block, one of the implemtnations
/// will be the WASM mapping.
pub trait BlockTransform {
    fn transform(&self, block: EncodedBlock, state: State) -> (State, Vec<EncodedTrigger>);
}

/// IndexerContext will provide all inputs necessary for the processing
pub struct IndexerContext<B: Blockchain, T: BlockTransform, S: IndexerStore> {
    chain: B,
    transform: T,
    store: S,
    deployment: DeploymentLocator,
}

impl<B: Blockchain, T: BlockTransform, S: IndexerStore> IndexerContext<B, T, S> {}

/// The IndexWorker glues all of the other types together and will manage the lifecycle
/// of the pre-indexing.
pub struct IndexWorker {}

impl IndexWorker {
    /// Performs the indexing work forever, or until the stop_block is reached. Run will
    /// start a new block_stream for the chain.
    async fn run<B, T, S>(
        &mut self,
        ctx: IndexerContext<B, T, S>,
        cursor_tracker: impl DeploymentCursorTracker,
        start_block: BlockNumber,
        stop_block: Option<BlockNumber>,
        filter: Arc<B::TriggerFilter>,
        api_version: UnifiedMappingApiVersion,
    ) -> Result<()>
    where
        B: Blockchain,
        T: BlockTransform,
        S: IndexerStore,
    {
        let mut block_stream = ctx
            .chain
            .new_block_stream(
                ctx.deployment.clone(),
                cursor_tracker,
                vec![start_block],
                filter,
                api_version,
            )
            .await?;
        if let Some(stop_block) = stop_block {
            block_stream = block_stream
                .take((stop_block - start_block) as usize)
                .into_inner();
        }

        self.process_stream(ctx, Box::pin(block_stream)).await?;

        Ok(())
    }

    async fn process_stream<B, T, S>(
        &mut self,
        ctx: IndexerContext<B, T, S>,
        mut stream: Pin<Box<impl Stream<Item = Result<BlockStreamEvent<B>, anyhow::Error>>>>,
    ) -> Result<FirehoseCursor>
    where
        B: Blockchain,
        T: BlockTransform,
        S: IndexerStore,
    {
        let mut firehose_cursor = FirehoseCursor::None;
        let mut previous_state = ctx.store.get_state().await?;
        loop {
            let evt = stream.next().await;

            let cursor = match evt {
                Some(Ok(BlockStreamEvent::ProcessWasmBlock(
                    block_ptr,
                    _block_time,
                    data,
                    _handler,
                    cursor,
                ))) => {
                    let (state, triggers) = ctx
                        .transform
                        .transform(EncodedBlock(data), std::mem::take(&mut previous_state));
                    previous_state = state;
                    ctx.store
                        .set(block_ptr.number, &previous_state, triggers)
                        .await?;

                    cursor
                }

                Some(Ok(BlockStreamEvent::ProcessBlock(block, cursor))) => {
                    unreachable!("Process block not implemented yet")
                }
                Some(Ok(BlockStreamEvent::Revert(revert_to_ptr, cursor))) => {
                    unimplemented!("revert not implemented yet")
                }
                Some(Err(e)) => return Err(e),

                None => break,
            };

            firehose_cursor = cursor;
        }

        Ok(firehose_cursor)
    }
}
