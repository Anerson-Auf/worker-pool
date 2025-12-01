use redis::{Client, aio::MultiplexedConnection};
use anyhow::Result;
use tracing::{info, error};
use redis::AsyncCommands;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;
use chrono::{DateTime, Utc};

use super::error::WorkerError;
use crate::shared::tasks::{TaskType, TaskWrapper};
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
        let wrapper = TaskWrapper{
            id: Uuid::new_v4(),
            task_type: task_type,
            retry_count: 0,
            created_at: Utc::now(),
        };
        let task_json = serde_json::to_string(&wrapper)?;
        let client = self.redis.as_ref().ok_or(WorkerError::RedisNotConnected)?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let _: i64 = conn.lpush("task-queue:default", &task_json).await?;
        info!("Enqueued task: {}", task_json);
        Ok(())
    }
    pub async fn start(self) -> Self {
        if let Some(redis) = self.redis.clone() {
            let mut conn = redis.get_multiplexed_async_connection().await.unwrap();
            conn.set::<&str, u64, ()>("task-queue:stats:active_workers", self.worker_count as u64).await.unwrap();
        }
        for _ in 0..self.worker_count {
            let redis_arc = self.redis.clone();
            tokio::spawn(async move {
                loop {
                    if let Some(redis) = redis_arc.as_ref() {
                        if let Err(e) = Self::process_next_task(redis.clone()).await {
                            error!("Worker error: {}", e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    } else {
                        error!("Redis not connected");
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
        }
        self
    }

    async fn process_task(task: TaskWrapper, conn: &mut MultiplexedConnection) -> Result<()> {
        let task_json = serde_json::to_string(&task)?;
        let task_type = task.task_type.clone();
        let result = task_type.process().await;
        if let Ok(()) = result {
            conn.lpush::<&str, String, ()>("task-queue:completed", task_json).await?;
            conn.incr::<&str, i64, ()>("task-queue:stats:completed_tasks", 1).await?;
        } else {
            let retry_count = task.retry_count + 1;
            if retry_count < 3 {
                let new_task = TaskWrapper{
                    id: task.id,
                    task_type: task.task_type,
                    retry_count: retry_count,
                    created_at: task.created_at,
                };
                let new_task_json = serde_json::to_string(&new_task)?;
                conn.lpush::<&str, String, ()>("task-queue:default", new_task_json).await?;
            } else {
                conn.lpush::<&str, String, ()>("task-queue:failed", task_json).await?;
            }
        }
        Ok(())
    }

    async fn process_next_task(redis: Arc<Client>) -> Result<()> {
        let mut conn = redis.get_multiplexed_async_connection().await?;
        let result: Option<(String, String)> = conn.brpop("task-queue:default", 10.0).await?;
        
        if let Some((_queue_name, task_json)) = result {
            let task: TaskWrapper = serde_json::from_str(&task_json)
                .map_err(|e| anyhow::anyhow!("Failed to deserialize task: {}", e))?;
            Self::process_task(task, &mut conn).await?;
        }
        
        Ok(())
    }
}