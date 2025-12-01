
mod client;
mod shared;
mod worker;

use crate::{shared::tasks::EmailTask, worker::types::Worker};
use anyhow::Result;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    dotenvy::dotenv().ok();
    let redis_url = dotenvy::var("REDIS_URL").expect("REDIS_URL must be set");
    let mut worker = Worker::new()
        .with_worker_count(4)
        .with_redis_connection(&redis_url).await?
        .start().await;
    
    worker.enqueue_task(EmailTask{
        email: "test@example.com".to_string(),
        subject: "Test Subject".to_string(),
        body: "Test Body".to_string(),
    }).await?;

    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");
    
    Ok(())
}
