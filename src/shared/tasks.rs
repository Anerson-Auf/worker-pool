use anyhow::{Result};
use serde::{Deserialize, Serialize};
use tracing::info;
use async_trait::async_trait;

#[async_trait]
pub trait Task: Serialize + for<'de> Deserialize<'de> {
    async fn process(self) -> Result<()>;
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TaskType {
    #[serde(rename = "email")]
    Email(EmailTask),
}

impl TaskType {
    pub async fn process(self) -> Result<()> {
        let result = match self {
            TaskType::Email(task) => task.process().await,
            _ => unreachable!("Unknown task type"),
        };
        result
    }
}

impl From<EmailTask> for TaskType {
    fn from(task: EmailTask) -> Self {
        TaskType::Email(task)
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct EmailTask {
    pub email: String,
    pub subject: String,
    pub body: String,
}

#[async_trait]
impl Task for EmailTask {
    async fn process(self) -> Result<()> {
        info!("Sending email to {} with subject {} and body {}", self.email, self.subject, self.body);
        Ok(())
    }
}