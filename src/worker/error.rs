use thiserror::Error;

#[derive(Error, Debug)]
pub enum WorkerError {
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),
    #[error("Redis not connected")]
    RedisNotConnected,
}