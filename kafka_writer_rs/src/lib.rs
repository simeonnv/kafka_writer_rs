use std::time::Duration;

use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::{
    sync::mpsc::{self, UnboundedSender},
    task::JoinHandle,
};

pub struct KafkaLogWriter {
    pub log_tx: UnboundedSender<String>,
    pub handle: JoinHandle<()>,
}

impl Drop for KafkaLogWriter {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl KafkaLogWriter {
    pub async fn new(kafka_client: FutureProducer, topic: &'static str) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();

        let handle = tokio::spawn(async move {
            while let Some(log) = rx.recv().await {
                let status = kafka_client
                    .send(
                        FutureRecord::to(topic)
                            .payload(&log)
                            .key(&format!("key-{}", uuid::Uuid::new_v4())),
                        Duration::from_secs(0),
                    )
                    .await;

                if let Err(e) = status {
                    eprintln!("failed sending log to kafka: {}", e.0);
                }
            }
        });

        Self { log_tx: tx, handle }
    }
}

impl std::io::Write for KafkaLogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let log_msg = String::from_utf8_lossy(buf).to_string();
        if let Err(e) = self.log_tx.send(log_msg) {
            eprintln!("failed sending log to kafka thread: {}", e);
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
