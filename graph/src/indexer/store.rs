use std::sync::Arc;

use crate::prelude::BlockNumber;
use crate::tokio::sync::RwLock;
use anyhow::{Error, Result};
use async_trait::async_trait;
use borsh::BorshDeserialize;
use sled::{Db, Tree};
use thiserror::Error;

use super::{EncodedTriggers, IndexerStore, State, StateDelta};
pub const DB_NAME: &str = "/media/data/sled_indexer_db";
pub const STATE_SNAPSHOT_FREQUENCY: u32 = 1000;

/// How frequently do we want state to be fully stored.
/// Never will prevent snapshots, this likely means state is not being used.
#[derive(Clone)]
pub enum StateSnapshotFrequency {
    Never,
    Blocks(u32),
}

impl Default for StateSnapshotFrequency {
    fn default() -> Self {
        Self::Blocks(STATE_SNAPSHOT_FREQUENCY)
    }
}

#[derive(Debug, Error)]
pub enum SledStoreError {
    #[error("sled returned an error: {0}")]
    SledError(#[from] sled::Error),
}

struct StoreInner {
    last_state_snapshot: Option<BlockNumber>,
}

#[derive(Clone)]
pub struct SledIndexerStore {
    tree: Tree,
    // Keeping interior mutability for now because of there is a chance we need to share the store
    // and the IndexingContext would definitely be shared
    inner: Arc<RwLock<StoreInner>>,
    snapshot_frequency: StateSnapshotFrequency,
}

impl SledIndexerStore {
    pub fn new(
        db: Arc<Db>,
        tree_name: &str,
        snapshot_frequency: StateSnapshotFrequency,
    ) -> Result<Self> {
        let tree = db.open_tree(tree_name).map_err(SledStoreError::from)?;
        let last_state_snapshot = tree
            .get(Self::latest_snapshot_key())
            .map_err(SledStoreError::from)?
            .map(|v| BlockNumber::from_le_bytes(v.to_vec().try_into().unwrap()));

        let inner = Arc::new(RwLock::new(StoreInner {
            last_state_snapshot,
        }));

        Ok(Self {
            tree,
            inner,
            snapshot_frequency,
        })
    }

    pub fn state_delta_key(bn: BlockNumber) -> String {
        format!("state_delta_{}", bn)
    }
    pub fn trigger_key(bn: BlockNumber) -> String {
        format!("trigger_{}", bn)
    }
    pub fn snapshot_key(bn: BlockNumber) -> String {
        format!("state_snapshot_{}", bn)
    }
    pub fn latest_snapshot_key() -> String {
        "latest_snapshot".to_string()
    }

    pub fn last_stable_block_key() -> String {
        "last_stable_block".to_string()
    }

    async fn should_snapshot(&self, bn: BlockNumber) -> bool {
        use StateSnapshotFrequency::*;

        let freq = match self.snapshot_frequency {
            Never => return false,
            Blocks(blocks) => blocks,
        };

        bn - self.inner.read().await.last_state_snapshot.unwrap_or(0) > freq.try_into().unwrap()
    }
}

#[async_trait]
impl IndexerStore for SledIndexerStore {
    async fn get(&self, bn: BlockNumber) -> Result<Option<EncodedTriggers>> {
        let v = self
            .tree
            .get(bn.to_string())
            .map_err(SledStoreError::from)?
            .map(|v| EncodedTriggers(v.to_vec().into_boxed_slice()));

        Ok(v)
    }
    async fn set_last_stable_block(&self, bn: BlockNumber) -> Result<()> {
        self.tree
            .insert(
                SledIndexerStore::last_stable_block_key().as_str(),
                bn.to_le_bytes().to_vec(),
            )
            .map_err(Error::from)?;

        self.tree
            .flush_async()
            .await
            .map(|_| ())
            .map_err(Error::from)
    }

    async fn set(&self, bn: BlockNumber, state: &State, triggers: EncodedTriggers) -> Result<()> {
        let should_snapshot = self.should_snapshot(bn).await;

        self.tree.transaction(|tx_db| {
            use sled::transaction::ConflictableTransactionError::*;

            tx_db
                .insert(
                    SledIndexerStore::state_delta_key(bn).as_str(),
                    borsh::to_vec(&state.delta()).unwrap(),
                )
                .map_err(Abort)?;

            tx_db
                .insert(
                    SledIndexerStore::trigger_key(bn).as_str(),
                    triggers.0.as_ref(),
                )
                .map_err(Abort)?;

            if should_snapshot {
                tx_db
                    .insert(
                        SledIndexerStore::snapshot_key(bn).as_str(),
                        borsh::to_vec(&state).unwrap(),
                    )
                    .map_err(Abort)?;

                tx_db
                    .insert(
                        SledIndexerStore::latest_snapshot_key().as_str(),
                        bn.to_le_bytes().to_vec(),
                    )
                    .map_err(Abort)?;
            }

            Ok(())
        })?;

        // This should always be a NOOP is state is not in use.
        if should_snapshot {
            self.inner.write().await.last_state_snapshot = Some(bn);
        }

        Ok(())
    }

    async fn get_state(&self, bn: BlockNumber) -> Result<State> {
        let block = match self.inner.read().await.last_state_snapshot {
            None => return Ok(State::default()),
            Some(block) => block,
        };

        let snapshot = self
            .tree
            .get(SledIndexerStore::snapshot_key(block))
            .map_err(SledStoreError::from)?
            .expect("last_state_snapshot doesn't match the marker");
        let mut state = State::try_from_slice(&snapshot.as_ref()).unwrap();

        self.tree
            .range(
                SledIndexerStore::state_delta_key(block + 1)..SledIndexerStore::state_delta_key(bn),
            )
            .collect::<Result<Vec<(_, _)>, _>>()?
            .iter()
            .for_each(|(_, v)| {
                let delta = StateDelta::try_from_slice(v.as_ref()).unwrap();

                state.apply(delta);
            });

        Ok(state)
    }
}
