use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use graph::prelude::BlockNumber;

use super::{EncodedTrigger, IndexerStore, State};

pub struct InMemoryIndexerStore {
    state: HashMap<BlockNumber, Vec<EncodedTrigger>>,
}

#[async_trait]
impl IndexerStore for InMemoryIndexerStore {
    async fn get(&self, bn: BlockNumber) -> Result<Vec<EncodedTrigger>> {
        unimplemented!();
    }
    async fn set(
        &self,
        bn: BlockNumber,
        state: &State,
        triggers: Vec<EncodedTrigger>,
    ) -> Result<()> {
        unimplemented!();
    }
    async fn get_state(&self) -> Result<State> {
        unimplemented!();
    }

    async fn get_all(&self, tag: &str) -> Result<Vec<EncodedTrigger>> {
        unimplemented!()
    }
}
