use std::pin::Pin;

use color_eyre::eyre::Result;
use futures::{Stream, TryStreamExt};
use sqlx::SqlitePool;

use crate::message::Message;

pub struct DbQuery {
    pool: SqlitePool,
}

impl DbQuery {
    pub fn as_stream(&self) -> Pin<Box<dyn Stream<Item = Result<Message>> + Send + '_>> {
        let query = sqlx::query_as::<_, MessageRow>(include_str!("../query.sql"));

        Box::pin(
            query
                .fetch(&self.pool)
                .map_err(|e| e.into())
                .and_then(|row| async move { Message::try_from(row) }),
        )
    }

    pub async fn as_vec(&self) -> Result<Vec<Message>> {
        let query = sqlx::query_as::<_, MessageRow>(include_str!("../query.sql"));

        let rows = query.fetch_all(&self.pool).await?;

        Ok(rows
            .into_iter()
            .map(Message::try_from)
            .fold(Vec::new(), |mut acc, msg| {
                if let Ok(msg) = msg {
                    acc.push(msg);
                }

                acc
            }))
    }

    // TODO: AsyncDrop when it stabilizes
    pub async fn drop(&self) {
        self.pool.close().await;
    }
}

pub async fn get_db_query() -> Result<DbQuery> {
    let chat_db_path =
        std::env::var("CHAT_DB_PATH").expect("CHAT_DB_PATH environment variable not set");

    Ok(DbQuery {
        pool: SqlitePool::connect(&format!("sqlite:{}?mode=ro", chat_db_path)).await?,
    })
}

#[derive(Debug, sqlx::FromRow)]
pub struct MessageRow {
    pub guid: String,
    pub date: Option<i64>,
    pub id: Option<String>,
    pub text: Option<String>,
    pub attributed_body: Option<Vec<u8>>,
    pub has_attachment: Option<i64>,
    pub balloon_bundle_id: Option<String>,
    pub thread_originator_guid: Option<String>,
}
