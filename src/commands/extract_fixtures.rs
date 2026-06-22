use color_eyre::eyre::{Context as _, Result, eyre};
use imessage_database::util::dirs::default_db_path;
use rusqlite::{Connection, OpenFlags, params};
use std::collections::HashMap;
use std::path::Path;

const TEST_MESSAGES: &[(&str, &str)] =
    &[("C0950D79-397C-462B-983C-600A51F3F1CA", "multiple images")];

const REQUIRED_TABLES: &[&str] = &[
    "handle",
    "chat",
    "message",
    "attachment",
    "message_attachment_join",
    "chat_message_join",
];

const SANITIZED_HANDLE: &str = "+10000000000";

pub fn run() -> Result<()> {
    if TEST_MESSAGES.is_empty() {
        println!("No messages configured in TEST_MESSAGES.");
        println!("\nTo add test messages:");
        println!("1. Send yourself test messages (emoji, wordle, image, etc.)");
        println!("2. Find the GUIDs with:");
        println!("   sqlite3 ~/Library/Messages/chat.db \\");
        println!(
            "     \"SELECT guid, text, balloon_bundle_id FROM message ORDER BY date DESC LIMIT 20\""
        );
        println!("3. Add them to TEST_MESSAGES in src/commands/extract_fixtures.rs");
        println!("4. Run: cargo run -- extract-fixtures");
        return Ok(());
    }

    let source_path = default_db_path();
    let source = Connection::open_with_flags(&source_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .wrap_err("failed to open source db")?;

    let dest_path = Path::new("tests/fixtures.db");
    if dest_path.exists() {
        std::fs::remove_file(dest_path)?;
    }
    let dest = Connection::open(dest_path)?;

    dest.execute("PRAGMA foreign_keys = OFF", [])?;

    println!("copying schema from source database...");
    copy_schema(&source, &dest)?;

    dest.execute(
        "INSERT INTO handle (ROWID, id, service) VALUES (1, ?1, 'iMessage')",
        params![SANITIZED_HANDLE],
    )?;

    let mut mapper = RowIdMapper::new();
    mapper.reserve("handle", 1);

    let mut success_count = 0;
    for (guid, description) in TEST_MESSAGES {
        print!("extracting: {guid} ({description})... ");
        match extract_message(&source, &dest, guid, &mut mapper) {
            Ok(()) => {
                println!("ok");
                success_count += 1;
            }
            Err(e) => println!("error: {}", e),
        }
    }

    println!(
        "\nextracted {}/{} messages to {}",
        success_count,
        TEST_MESSAGES.len(),
        dest_path.display()
    );
    Ok(())
}

fn copy_schema(source: &Connection, dest: &Connection) -> Result<()> {
    for table in REQUIRED_TABLES {
        let create_sql: String = source
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
                params![table],
                |row| row.get(0),
            )
            .wrap_err_with(|| eyre!("table '{table}' not found in source database"))?;

        dest.execute(&create_sql, [])
            .wrap_err_with(|| eyre!("failed to create table '{table}'"))?;
    }
    Ok(())
}

fn get_column_names(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info('{}')", table))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .filter_map(|r| r.ok())
        .filter(|name| name.to_uppercase() != "ROWID") // Exclude ROWID, we handle it separately
        .collect();
    Ok(columns)
}

struct RowIdMapper {
    maps: HashMap<String, HashMap<i64, i64>>,
    counters: HashMap<String, i64>,
}

impl RowIdMapper {
    fn new() -> Self {
        Self {
            maps: HashMap::new(),
            counters: HashMap::new(),
        }
    }

    fn reserve(&mut self, table: &str, rowid: i64) {
        let counter = self.counters.entry(table.to_string()).or_insert(0);
        if rowid > *counter {
            *counter = rowid;
        }
        self.maps
            .entry(table.to_string())
            .or_default()
            .insert(rowid, rowid);
    }

    fn map(&mut self, table: &str, old_rowid: i64) -> i64 {
        let table_map = self.maps.entry(table.to_string()).or_default();
        if let Some(&new_id) = table_map.get(&old_rowid) {
            return new_id;
        }

        let counter = self.counters.entry(table.to_string()).or_insert(0);
        *counter += 1;
        let new_id = *counter;
        table_map.insert(old_rowid, new_id);
        new_id
    }

    fn get(&self, table: &str, old_rowid: i64) -> Option<i64> {
        self.maps.get(table)?.get(&old_rowid).copied()
    }
}

fn copy_row(
    source: &Connection,
    dest: &Connection,
    table: &str,
    where_clause: &str,
    where_params: &[i64],
    mapper: &mut RowIdMapper,
) -> Result<Option<i64>> {
    let columns = get_column_names(source, table)?;
    let cols_str = columns.join(", ");

    let select_sql = format!(
        "SELECT ROWID, {} FROM {} WHERE {}",
        cols_str, table, where_clause
    );

    let where_params_refs: Vec<&dyn rusqlite::ToSql> = where_params
        .iter()
        .map(|p| p as &dyn rusqlite::ToSql)
        .collect();

    let row_data = source.query_row(&select_sql, where_params_refs.as_slice(), |row| {
        let old_rowid: i64 = row.get(0)?;
        let new_rowid = mapper.map(table, old_rowid);

        let mut values: Vec<rusqlite::types::Value> = vec![new_rowid.into()];

        for (i, col_name) in columns.iter().enumerate() {
            let col_idx = i + 1; // offset by 1 due to ROWID

            let value: rusqlite::types::Value = match (table, col_name.as_str()) {
                // Sanitize handle references -> point to ROWID 1
                ("message", "handle_id") | ("message", "other_handle") => 1i64.into(),
                // Sanitize personally identifiable fields
                ("message", "destination_caller_id") | ("message", "account") => {
                    rusqlite::types::Value::Null
                }
                ("message_attachment_join", "message_id") => {
                    let old: i64 = row.get(col_idx)?;
                    mapper.get("message", old).unwrap_or(old).into()
                }
                ("message_attachment_join", "attachment_id") => {
                    let old: i64 = row.get(col_idx)?;
                    mapper.get("attachment", old).unwrap_or(old).into()
                }
                ("chat_message_join", "message_id") => {
                    let old: i64 = row.get(col_idx)?;
                    mapper.get("message", old).unwrap_or(old).into()
                }
                ("chat_message_join", "chat_id") => {
                    let old: i64 = row.get(col_idx)?;
                    mapper.get("chat", old).unwrap_or(old).into()
                }
                _ => row.get_ref(col_idx)?.try_into()?,
            };
            values.push(value);
        }

        Ok((new_rowid, values))
    });

    match row_data {
        Ok((new_rowid, values)) => {
            let placeholders: Vec<String> = (1..=values.len()).map(|i| format!("?{i}")).collect();

            let insert_sql = format!(
                "INSERT OR IGNORE INTO {} (ROWID, {}) VALUES ({})",
                table,
                columns.join(", "),
                placeholders.join(", ")
            );

            let params = values
                .iter()
                .map(|v| v as &dyn rusqlite::ToSql)
                .collect::<Vec<_>>();

            dest.execute(&insert_sql, params.as_slice())?;
            Ok(Some(new_rowid))
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn extract_message(
    source: &Connection,
    dest: &Connection,
    guid: &str,
    mapper: &mut RowIdMapper,
) -> Result<()> {
    let old_msg_rowid: i64 = source
        .query_row(
            "SELECT ROWID FROM message WHERE guid = ?1",
            params![guid],
            |row| row.get(0),
        )
        .wrap_err_with(|| eyre!("message not found: {guid}"))?;

    copy_row(
        source,
        dest,
        "message",
        "ROWID = ?1",
        &[old_msg_rowid],
        mapper,
    )?;

    let mut attachment_stmt = source
        .prepare("SELECT attachment_id FROM message_attachment_join WHERE message_id = ?1")?;
    let attachment_ids: Vec<i64> = attachment_stmt
        .query_map(params![old_msg_rowid], |row| row.get(0))?
        .collect::<std::result::Result<_, _>>()?;

    for old_att_id in attachment_ids {
        copy_row(
            source,
            dest,
            "attachment",
            "ROWID = ?1",
            &[old_att_id],
            mapper,
        )?;

        copy_row(
            source,
            dest,
            "message_attachment_join",
            "message_id = ?1 AND attachment_id = ?2",
            &[old_msg_rowid, old_att_id],
            mapper,
        )?;
    }

    let chat_info: Option<i64> = source
        .query_row(
            "SELECT chat_id FROM chat_message_join WHERE message_id = ?1",
            params![old_msg_rowid],
            |row| row.get(0),
        )
        .ok();

    if let Some(old_chat_id) = chat_info {
        copy_row(source, dest, "chat", "ROWID = ?1", &[old_chat_id], mapper)?;

        copy_row(
            source,
            dest,
            "chat_message_join",
            "message_id = ?1 AND chat_id = ?2",
            &[old_msg_rowid, old_chat_id],
            mapper,
        )?;
    }

    Ok(())
}
