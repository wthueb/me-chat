use std::collections::{BTreeSet, HashMap, HashSet};

use color_eyre::eyre::{Result, eyre};
use imessage_database::{
    tables::{
        chat::Chat,
        handle::Handle,
        messages::Message as DbMessage,
        table::{Cacheable as _, Table as _},
    },
    util::query_context::QueryContext,
};
use ouroboros::self_referencing;
use rusqlite::Connection;

use crate::message::Message;

pub struct Db<'a> {
    conn: &'a Connection,
    query_context: QueryContext,
    handles: HashMap<i32, String>,
}

impl<'a> Db<'a> {
    pub fn new(conn: &'a Connection, group_guids: &HashSet<&str>) -> Result<Self> {
        let handles = Handle::cache(conn).map_err(|e| eyre!("failed to cache handles: {e}"))?;

        let group_ids = Chat::get(conn)
            .map_err(|e| eyre!("failed to get chats: {e}"))?
            .query_map([], |row| {
                Ok((
                    row.get::<_, i32>("ROWID")?,
                    row.get::<_, Option<String>>("group_id")?,
                ))
            })
            .map_err(|e| eyre!("failed to map chats: {e}"))?
            .filter_map(|res| {
                if let Ok((id, group_id)) = res
                    && let Some(group_id) = group_id
                    && group_guids.contains(group_id.as_str())
                {
                    Some(id)
                } else {
                    None
                }
            })
            .collect::<BTreeSet<_>>();

        let query_context = QueryContext {
            selected_chat_ids: Some(group_ids),
            ..Default::default()
        };

        Ok(Self {
            conn,
            query_context,
            handles,
        })
    }

    pub fn get_count(&self) -> Result<i64> {
        DbMessage::get_count(self.conn, &self.query_context)
            .map_err(|e| eyre!("failed to count messages: {e}"))
    }

    pub fn stream<F>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(Result<Message>) -> Result<()>,
    {
        let mut statement = DbMessage::stream_rows(self.conn, &self.query_context)
            .map_err(|e| eyre!("failed to stream messages: {e}"))?;

        let messages = statement
            .query_map([], DbMessage::from_row)?
            .filter_map(Result::ok);

        for mut raw in messages {
            let _ = raw.generate_text(self.conn);
            let msg = Message::from_raw(raw, self.conn, &self.handles);
            callback(msg)?;
        }

        Ok(())
    }

    pub fn iter_messages(&self) -> Result<MessageIterator<'_>> {
        let statement = DbMessage::stream_rows(self.conn, &self.query_context)
            .map_err(|e| eyre!("failed to generate messages statement: {e}"))?;

        let inner = MessageIteratorInnerTryBuilder {
            statement,
            rows_builder: |stmt| stmt.query([]),
        }
        .try_build()
        .map_err(|e| eyre!("failed to create message iterator: {e}"))?;

        Ok(MessageIterator {
            inner,
            conn: self.conn,
            handles: &self.handles,
        })
    }
}

#[self_referencing]
struct MessageIteratorInner<'a> {
    statement: rusqlite::CachedStatement<'a>,
    #[borrows(mut statement)]
    #[covariant]
    rows: rusqlite::Rows<'this>,
}

pub struct MessageIterator<'a> {
    inner: MessageIteratorInner<'a>,
    conn: &'a Connection,
    handles: &'a HashMap<i32, String>,
}

impl<'a> Iterator for MessageIterator<'a> {
    type Item = Result<Message>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.inner.with_rows_mut(|rows| {
            rows.next()
                .transpose()
                .map(|res| res.and_then(DbMessage::from_row))
        });

        match result {
            Some(Ok(mut raw)) => {
                let _ = raw.generate_text(self.conn);
                Some(Message::from_raw(raw, self.conn, self.handles))
            }
            Some(Err(e)) => {
                panic!("failed to get next message: {e}");
            }
            None => None,
        }
    }
}
