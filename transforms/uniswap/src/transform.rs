use std::str::FromStr;

use borsh::{BorshDeserialize, BorshSerialize};
use ethabi::Address;
use prost::Message;
use substreams_ethereum::pb::eth::v2::Block;

use graph::indexer::{BlockTransform, EncodedBlock, EncodedTriggers, State};

use crate::abi::factory::events::PoolCreated;

const UNISWAP_V2_FACTORY: &str = "0x1F98431c8aD98523631AE4a59f267346ea31F984";
pub const POOL_TAG: &str = "pool";

#[derive(BorshSerialize, BorshDeserialize)]
pub struct Pool {
    address: Vec<u8>,
    token0: Vec<u8>,
    token1: Vec<u8>,
    owner: Vec<u8>,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct Events {
    events: Vec<Event>,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub enum Event {
    PoolCreated(Pool),
}

#[derive(Clone, Debug)]
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
    fn transform(&self, block: EncodedBlock, mut state: State) -> (State, EncodedTriggers) {
        let mut events = vec![];
        let block = Block::decode(block.0.as_ref()).unwrap();

        for log in block.logs() {
            if log.log.block_index == 0 {
                continue;
            }

            if log.address() != self.factory_addr.as_ref() {
                continue;
            }

            match PoolCreated::decode(&log.log) {
                Ok(PoolCreated {
                    token0,
                    token1,
                    fee: _,
                    tick_spacing: _,
                    pool,
                }) => {
                    let p = Pool {
                        address: pool,
                        token0,
                        token1,
                        owner: log.address().to_vec(),
                    };

                    events.push(Event::PoolCreated(p));
                }
                _ => continue,
            }
        }

        let bs = borsh::to_vec(&Events { events }).unwrap();

        (state, EncodedTriggers(bs.into_boxed_slice()))
    }
}
