use core::pin::Pin;
use core::task::{Context, Poll};
use futures::Stream;
use pin_project_lite::pin_project;
use tracing::info;

use super::{BatchBoundary, BatchConfig};

// Implementation adapted from https://github.com/supabase/pg_replicate/blob/main/pg_replicate/src/pipeline/batching/stream.rs
pin_project! {
    /// Adapter stream which batches the items of the underlying stream when it
    /// reaches the an item that returns true from [`BatchBoundary::is_last_in_batch`] or
    /// when the batch size reaches the maximum batch size. Only items that return true from [`BatchBoundary::is_batch_relevant`]
    /// are counted towards the batch size.
    /// The underlying stream items must implement [`BatchBoundary`].
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
    pub struct TransactionBatchStream<B: BatchBoundary, S: Stream<Item = B>> {
        #[pin]
        stream: S,
        items: Vec<S::Item>,
        inner_stream_ended: bool,
        batch_config: BatchConfig,
    }
}

impl<B: BatchBoundary, S: Stream<Item = B>> TransactionBatchStream<B, S> {
    pub fn new(stream: S, batch_config: BatchConfig) -> Self {
        TransactionBatchStream {
            stream,
            items: Vec::with_capacity(batch_config.max_batch_size),
            inner_stream_ended: false,
            batch_config,
        }
    }

    pub fn get_inner_mut(&mut self) -> &mut S {
        &mut self.stream
    }
}

impl<B: BatchBoundary, S: Stream<Item = B>> Stream for TransactionBatchStream<B, S> {
    type Item = Vec<S::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        if *this.inner_stream_ended {
            return Poll::Ready(None);
        }

        loop {
            if this.items.is_empty() {
                this.items.reserve_exact(this.batch_config.max_batch_size);
            }
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => break,
                Poll::Ready(Some(item)) => {
                    let is_last_in_batch = item.is_last_in_batch();
                    this.items.push(item);
                    if is_last_in_batch
                        || this.items.iter().filter(|i| i.is_batch_relevant()).count()
                            >= this.batch_config.max_batch_size
                    {
                        return Poll::Ready(Some(std::mem::take(this.items)));
                    }
                }
                Poll::Ready(None) => {
                    let last = if this.items.is_empty() {
                        None
                    } else {
                        Some(std::mem::take(this.items))
                    };

                    *this.inner_stream_ended = true;

                    return Poll::Ready(last);
                }
            }
        }

        if !this.items.is_empty() {
            let last_item = this.items.last().expect("missing last item");
            if last_item.is_last_in_batch() {
                return Poll::Ready(Some(std::mem::take(this.items)));
            }
        }

        Poll::Pending
    }
}
