use crate::blockchain::block_stream::{
    BlockStream, BlockStreamEvent, BlockStreamMapper, FirehoseCursor,
};
use crate::blockchain::{BlockTime, Blockchain};
use crate::prelude::*;
use crate::util::backoff::ExponentialBackoff;
use async_stream::try_stream;
use futures03::{Stream, StreamExt};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use super::IndexerStore;

pub const INDEXER_STREAM_BUFFER_STREAM_SIZE: usize = 10;

pub struct IndexerBlockStream<C: Blockchain> {
    //fixme: not sure if this is ok to be set as public, maybe
    // we do not want to expose the stream to the caller
    stream: Pin<Box<dyn Stream<Item = Result<BlockStreamEvent<C>, Error>> + Send>>,
}

impl<C> IndexerBlockStream<C>
where
    C: Blockchain,
{
    pub fn new(
        deployment: DeploymentHash,
        store: Arc<dyn IndexerStore>,
        subgraph_current_block: Option<BlockPtr>,
        start_blocks: Vec<BlockNumber>,
        end_blocks: Vec<BlockNumber>,
        logger: Logger,
        registry: Arc<MetricsRegistry>,
    ) -> Self {
        let metrics = IndexerStreamMetrics::new(registry, deployment.clone());

        let start_block = start_blocks.iter().min();
        let start_block = match (subgraph_current_block, start_block) {
            (None, None) => 0,
            (None, Some(i)) => *i,
            (Some(ptr), _) => ptr.number,
        };

        IndexerBlockStream {
            stream: Box::pin(stream_blocks(
                deployment,
                store,
                logger,
                metrics,
                start_block,
                end_blocks.iter().min().map(|n| *n),
            )),
        }
    }
}

fn stream_blocks<C: Blockchain>(
    deployment: DeploymentHash,
    store: Arc<dyn IndexerStore>,
    logger: Logger,
    metrics: IndexerStreamMetrics,
    start_block: BlockNumber,
    stop_block: Option<BlockNumber>,
) -> impl Stream<Item = Result<BlockStreamEvent<C>, Error>> {
    // Back off exponentially whenever we encounter a connection error or a stream with bad data
    let mut backoff = ExponentialBackoff::new(Duration::from_millis(500), Duration::from_secs(45));
    let stop_block = stop_block.unwrap_or(i32::MAX);

    // This attribute is needed because `try_stream!` seems to break detection of `skip_backoff` assignments
    #[allow(unused_assignments)]
    let mut skip_backoff = false;

    try_stream! {
            let logger = logger.new(o!("deployment" => deployment.clone()));

        loop {
            info!(
                &logger,
                "IndexerStream starting";
                "subgraph" => &deployment,
                "start_block" => start_block,
                "stop_block" => stop_block,
            );

            // We just reconnected, assume that we want to back off on errors
            skip_backoff = false;

            let (tx, mut rx) = mpsc::channel(10);
            let stream = store.stream_from(start_block, tx);


            info!(&logger, "IndexerStream started");

            // Track the time it takes to set up the block stream
            // metrics.observe_successful_connection(&mut connect_start, &endpoint.provider);

            loop {
                let response = rx.recv().await;
                match response {
                    None => break,
                    Some((bn, triggers)) => {
                        let block_ptr = BlockPtr::new(BlockHash::zero(), bn);


                        let evt = BlockStreamEvent::ProcessWasmBlock(block_ptr, BlockTime::NONE, triggers.0, "".to_string(), FirehoseCursor::None);
                        yield evt;
                    }
                  }
            }

        }
    }
}

impl<C: Blockchain> Stream for IndexerBlockStream<C> {
    type Item = Result<BlockStreamEvent<C>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl<C: Blockchain> BlockStream<C> for IndexerBlockStream<C> {
    fn buffer_size_hint(&self) -> usize {
        INDEXER_STREAM_BUFFER_STREAM_SIZE
    }
}

struct IndexerStreamMetrics {
    deployment: DeploymentHash,
    restarts: CounterVec,
    connect_duration: GaugeVec,
    time_between_responses: HistogramVec,
    responses: CounterVec,
}

impl IndexerStreamMetrics {
    pub fn new(registry: Arc<MetricsRegistry>, deployment: DeploymentHash) -> Self {
        Self {
            deployment,
            restarts: registry
                .global_counter_vec(
                    "deployment_substreams_blockstream_restarts",
                    "Counts the number of times a Substreams block stream is (re)started",
                    vec!["deployment", "provider", "success"].as_slice(),
                )
                .unwrap(),

            connect_duration: registry
                .global_gauge_vec(
                    "deployment_substreams_blockstream_connect_duration",
                    "Measures the time it takes to connect a Substreams block stream",
                    vec!["deployment", "provider"].as_slice(),
                )
                .unwrap(),

            time_between_responses: registry
                .global_histogram_vec(
                    "deployment_substreams_blockstream_time_between_responses",
                    "Measures the time between receiving and processing Substreams stream responses",
                    vec!["deployment", "provider"].as_slice(),
                )
                .unwrap(),

            responses: registry
                .global_counter_vec(
                    "deployment_substreams_blockstream_responses",
                    "Counts the number of responses received from a Substreams block stream",
                    vec!["deployment", "provider", "kind"].as_slice(),
                )
                .unwrap(),
        }
    }

    fn observe_successful_connection(&self, time: &mut Instant, provider: &str) {
        self.restarts
            .with_label_values(&[&self.deployment, &provider, "true"])
            .inc();
        self.connect_duration
            .with_label_values(&[&self.deployment, &provider])
            .set(time.elapsed().as_secs_f64());

        // Reset last connection timestamp
        *time = Instant::now();
    }

    fn observe_failed_connection(&self, time: &mut Instant, provider: &str) {
        self.restarts
            .with_label_values(&[&self.deployment, &provider, "false"])
            .inc();
        self.connect_duration
            .with_label_values(&[&self.deployment, &provider])
            .set(time.elapsed().as_secs_f64());

        // Reset last connection timestamp
        *time = Instant::now();
    }

    fn observe_response(&self, kind: &str, time: &mut Instant, provider: &str) {
        self.time_between_responses
            .with_label_values(&[&self.deployment, &provider])
            .observe(time.elapsed().as_secs_f64());
        self.responses
            .with_label_values(&[&self.deployment, &provider, kind])
            .inc();

        // Reset last response timestamp
        *time = Instant::now();
    }
}
