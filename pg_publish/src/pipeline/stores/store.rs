use std::sync::{atomic::AtomicU64, Arc};

use tokio::sync::{mpsc, oneshot};
use tokio_postgres::types::PgLsn;
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::{StoreAdapter, StoreError};

pub enum StoreMessage {
    GetLatestLsn { respond_to: oneshot::Sender<PgLsn> },
    GetLatestCommittedLsn { respond_to: oneshot::Sender<PgLsn> },
    SucessLsn { lsn: PgLsn },
    Flush {},
}

pub struct Store<A: StoreAdapter> {
    store_rx: mpsc::Receiver<StoreMessage>,
    last_lsn: Arc<AtomicU64>,
    adapter: A,
    flush_interval: u64,
    cancel: CancellationToken,
}

impl<A: StoreAdapter + 'static> Store<A> {
    pub fn new(
        store_rx: mpsc::Receiver<StoreMessage>,
        adapter: A,
        flush_interval: u64,
        cancel: CancellationToken,
        initial_lsn: PgLsn,
    ) -> Self {
        Self {
            store_rx,
            last_lsn: Arc::new(AtomicU64::new(initial_lsn.into())),
            adapter,
            flush_interval,
            cancel,
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.store_rx.recv() => {
                    let _ = self.handle_message(msg).await;
                },
                _ = self.cancel.cancelled() => {
                    self.store_rx.close();
                    while let Some(msg) = self.store_rx.recv().await {
                        let _ = self.handle_message(msg).await;
                    }
                    let _ = self.handle_message(StoreMessage::Flush{}).await;
                    break;
                },
                else => break,
            }
        }
    }

    async fn handle_message(&mut self, msg: StoreMessage) -> Result<(), StoreError> {
        match msg {
            StoreMessage::Flush {} => {
                self.adapter
                    .write_lsn(
                        self.last_lsn
                            .load(std::sync::atomic::Ordering::SeqCst)
                            .into(),
                    )
                    .await?;
            }
            StoreMessage::SucessLsn { lsn } => {
                self.last_lsn
                    .fetch_max(lsn.into(), std::sync::atomic::Ordering::SeqCst);
            }
            StoreMessage::GetLatestCommittedLsn { respond_to } => {
                let lsn = self.adapter.read_lsn().await?;
                let _ = respond_to.send(lsn);
            }
            StoreMessage::GetLatestLsn { respond_to } => {
                let _ = respond_to.send(PgLsn::from(
                    self.last_lsn.load(std::sync::atomic::Ordering::SeqCst),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct StoreHandle<A: StoreAdapter> {
    sender: mpsc::Sender<StoreMessage>,
    cancel: CancellationToken,
    _adapter: std::marker::PhantomData<A>,
    cancel_complete: Arc<tokio::sync::Notify>,
}

impl<A: StoreAdapter + 'static> StoreHandle<A> {
    pub async fn new(adapter: A) -> Result<Self, StoreError> {
        let (sender, receiver) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let cancel_complete = Arc::new(tokio::sync::Notify::new());

        let initial_lsn = adapter.read_lsn().await?;

        let mut actor = Store::new(receiver, adapter, 10, cancel.clone(), initial_lsn);

        let cancel_complete_clone = cancel_complete.clone();

        tokio::spawn(async move {
            actor.run().await;
            cancel_complete_clone.notify_one();
        });

        Ok(Self {
            sender,
            cancel,
            _adapter: std::marker::PhantomData,
            cancel_complete,
        })
    }

    pub async fn report_lsn(&self, lsn: PgLsn) {
        let msg = StoreMessage::SucessLsn { lsn };
        let _ = self.sender.send(msg).await;
    }

    pub async fn get_latest_committed_lsn(&self) -> PgLsn {
        let (send, recv) = oneshot::channel();
        let msg = StoreMessage::GetLatestCommittedLsn { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Store task has been killed")
    }

    pub async fn get_latest_lsn(&self) -> PgLsn {
        let (send, recv) = oneshot::channel();
        let msg = StoreMessage::GetLatestLsn { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Store task has been killed")
    }

    pub async fn shutdown(&self) {
        self.cancel.cancel();
        self.cancel_complete.notified().await;
    }
}
