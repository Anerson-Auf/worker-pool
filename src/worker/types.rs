use redis::Client;
use anyhow::Result;
use tracing::{info};
use redis::AsyncCommands;
use std::{sync::Arc, time::Duration};

use super::error::WorkerError;
use crate::shared::tasks::TaskType;
pub struct Worker {
    pub redis: Option<Arc<Client>>,
    pub worker_count: usize,
}
impl Worker {
    pub fn new() -> Self {
        Self {
            redis: None,
            worker_count: 1,
        }
    }
    pub fn with_worker_count(mut self, worker_count: usize) -> Self {
        assert!(worker_count > 0, "Worker count must be greater than 0");
        self.worker_count = worker_count;
        info!("Init {} workers.", worker_count);
        self
    }
    pub async fn with_redis_connection(mut self, redis_url: &str) -> Result<Self, WorkerError> {
        let client = Client::open(redis_url)?;
        self.redis = Some(Arc::new(client));
        info!("Connected to redis.");
        Ok(self)
    }
    pub async fn enqueue_task<T>(&mut self, task: T) -> Result<()>
    where
        T: Into<TaskType>,
    {
        let task_type: TaskType = task.into();
        let task_json = serde_json::to_string(&task_type)?;
        let client = self.redis.as_ref().ok_or(WorkerError::RedisNotConnected)?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let _: i64 = conn.lpush("task-queue:default", &task_json).await?;
        info!("Enqueued task: {}", task_json);
        Ok(())
    }
    pub fn start(self) -> Self {
        for _ in 0..self.worker_count {
            let redis_arc = self.redis.clone();
            tokio::spawn(async move {
                loop {
                    if let Some(redis) = redis_arc.as_ref() {
                        if let Err(e) = Self::process_next_task(redis.clone()).await {
                            tracing::error!("Worker error: {}", e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    } else {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            });
        }
        self
    }

    async fn process_next_task(redis: Arc<Client>) -> Result<()> {
        let mut conn = redis.get_multiplexed_async_connection().await?;
        let result: Option<(String, String)> = conn.brpop("task-queue:default", 10.0).await?;
        
        if let Some((_queue_name, task_json)) = result {
            let task: TaskType = serde_json::from_str(&task_json)
                .map_err(|e| anyhow::anyhow!("Failed to deserialize task: {}", e))?;
            task.process().await?;
        }
        
        Ok(())
    }
}