use std::str::FromStr;

use borsh::{BorshDeserialize, BorshSerialize};
use ethabi::Address;
use graph::prelude::prost::Message;
use substreams_ethereum::pb::eth::v2::Block;

use crate::indexer::{BlockTransform, EncodedBlock, EncodedTrigger, State};

use super::abi::factory::events::PairCreated;

const UNISWAP_V2_FACTORY: &str = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73";
pub const POOL_TAG: &str = "pool";

#[derive(BorshSerialize, BorshDeserialize)]
pub struct Pool {
    address: Vec<u8>,
    token0: Vec<u8>,
    token1: Vec<u8>,
}

pub struct UniswapTransform {
    factory_addr: Address,
}

impl UniswapTransform {
    #[allow(unused)]
    pub fn new() -> Self {
        Self {
            factory_addr: Address::from_str(UNISWAP_V2_FACTORY).unwrap(),
        }
    }
}

impl BlockTransform for UniswapTransform {
    fn transform(&self, block: EncodedBlock, mut state: State) -> (State, Vec<EncodedTrigger>) {
        let res = vec![];
        let block = Block::decode(block.0.as_ref()).unwrap();

        for log in block.logs() {
            if log.log.block_index == 0 {
                continue;
            }

            if log.address() != self.factory_addr.as_ref() {
                continue;
            }

            match PairCreated::decode(&log.log) {
                Ok(PairCreated {
                    token0,
                    token1,
                    pair: _,
                    param3: _,
                }) => {
                    let p = Pool {
                        address: log.address().to_owned(),
                        token0,
                        token1,
                    };

                    state
                        .set_encode(
                            crate::indexer::Key {
                                id: log.address().into(),
                                tag: Some(POOL_TAG),
                            },
                            p,
                        )
                        .unwrap();
                }
                _ => continue,
            }
        }

        let all_pools = state.get_keys(POOL_TAG);
        for log in block.logs() {
            // Get relevant pool events for all pools
        }

        (state, res)
    }
}
