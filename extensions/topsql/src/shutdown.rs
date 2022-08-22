use async_recursion::async_recursion;
use tokio::sync::watch;

pub fn pair() -> (ShutdownNotifier, ShutdownSubscriber) {
    let (tx, rx) = watch::channel(());
    (
        ShutdownNotifier { tx },
        ShutdownSubscriber { parent: None, rx },
    )
}

pub struct ShutdownNotifier {
    tx: watch::Sender<()>,
}

impl ShutdownNotifier {
    pub fn shutdown(&self) {
        let _ = self.tx.send(());
    }

    pub async fn wait_for_exit(&self) {
        self.tx.closed().await;
    }
}

#[derive(Clone)]
pub struct ShutdownSubscriber {
    parent: Option<Box<ShutdownSubscriber>>,
    rx: watch::Receiver<()>,
}

impl ShutdownSubscriber {
    #[async_recursion]
    pub async fn done(&mut self) {
        let rx = &mut self.rx;
        match self.parent.as_mut() {
            None => {
                let _ = rx.changed().await;
            }
            Some(parent) => {
                let parent = parent.as_mut();
                tokio::select! {
                    _ = parent.done() => {}
                    _ = rx.changed() => {}
                }
            }
        }
    }

    pub fn extend(&self) -> (ShutdownNotifier, ShutdownSubscriber) {
        let (tx, rx) = watch::channel(());
        (
            ShutdownNotifier { tx },
            ShutdownSubscriber {
                parent: Some(Box::new(self.clone())),
                rx,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn ten_subscribers() {
        let (notifier, subscriber) = pair();

        const COUNT: usize = 10;
        let done = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];
        for _ in 0..COUNT {
            let done = done.clone();
            let mut subscriber = subscriber.clone();
            handles.push(tokio::spawn(async move {
                subscriber.done().await;
                done.fetch_add(1, Ordering::SeqCst);
            }));
        }
        drop(subscriber);

        notifier.shutdown();
        notifier.wait_for_exit().await;
        assert_eq!(done.load(Ordering::SeqCst), COUNT);

        let _ = futures::future::join_all(handles).await;
    }

    #[tokio::test]
    async fn no_subscribers() {
        let (notifier, _) = pair();

        notifier.shutdown();
        notifier.wait_for_exit().await;
    }

    #[tokio::test]
    async fn subscribers_drop_before_wait() {
        let (notifier, subscriber) = pair();

        let mut handles = vec![];
        for _ in 0..5 {
            let subscriber = subscriber.clone();
            handles.push(tokio::spawn(async move {
                let _s = subscriber;
            }));
        }
        drop(subscriber);

        notifier.shutdown();
        notifier.wait_for_exit().await;

        let _ = futures::future::join_all(handles).await;
    }

    #[tokio::test]
    async fn notifier_drop_after_spawn() {
        let (notifier, subscriber) = pair();

        let mut handles = vec![];
        for _ in 0..5 {
            let mut subscriber = subscriber.clone();
            handles.push(tokio::spawn(async move {
                subscriber.done().await;
            }));
        }
        drop((notifier, subscriber));

        let _ = futures::future::join_all(handles).await;
    }

    #[tokio::test]
    async fn notifier_drop_before_spawn() {
        let (notifier, subscriber) = pair();

        drop(notifier);
        let mut handles = vec![];
        for _ in 0..5 {
            let mut subscriber = subscriber.clone();
            handles.push(tokio::spawn(async move {
                subscriber.done().await;
            }));
        }
        drop(subscriber);

        let _ = futures::future::join_all(handles).await;
    }

    #[tokio::test]
    async fn really_wait_for_exit() {
        let (notifier, mut subscriber) = pair();

        let (cont_tx, mut cont_rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = tokio::spawn(async move {
            let _ = cont_rx.recv().await;
            subscriber.done().await;
        });

        notifier.shutdown();

        // subscriber is blocked on something and cannot exit, so wait_for_exit is also blocked
        assert!(
            timeout(std::time::Duration::from_secs(1), notifier.wait_for_exit())
                .await
                .is_err()
        );

        // unblock subscriber and wait_for_exit should act well
        let _ = cont_tx.send(());
        notifier.wait_for_exit().await;

        let _ = handle.await;
    }

    #[tokio::test]
    async fn nested_inner_shutdown() {
        let (notifier, subscriber) = pair();

        let handle = tokio::spawn(async move {
            let (sub_notifier, mut sub_subscriber) = subscriber.extend();

            let handle = tokio::spawn(async move {
                sub_subscriber.done().await;
            });

            sub_notifier.shutdown();
            sub_notifier.wait_for_exit().await;
            let _ = handle.await;
        });

        notifier.wait_for_exit().await;
        let _ = handle.await;
    }

    #[tokio::test]
    async fn nested_outer_shutdown() {
        let (notifier, subscriber) = pair();

        let mut handles = vec![];
        for _ in 0..3 {
            let mut subscriber = subscriber.clone();
            handles.push(tokio::spawn(async move {
                let mut handles = vec![];
                {
                    let (sub_notifier, sub_subscriber) = subscriber.extend();
                    for _ in 0..3 {
                        let mut subscriber = sub_subscriber.clone();
                        handles.push(tokio::spawn(async move {
                            subscriber.done().await;
                        }));
                    }
                    drop(sub_subscriber);
                    sub_notifier.wait_for_exit().await;
                }

                subscriber.done().await;
                let _ = futures::future::join_all(handles).await;
            }));
        }
        drop(subscriber);

        notifier.shutdown();
        notifier.wait_for_exit().await;
        let _ = futures::future::join_all(handles).await;
    }
}
