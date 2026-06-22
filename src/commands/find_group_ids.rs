use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use color_eyre::eyre::{Context as _, Result};
use imessage_database::{
    tables::{
        handle::Handle,
        messages::Message as DbMessage,
        table::{Cacheable as _, Table as _, get_connection},
    },
    util::{dates::get_offset, dirs::default_db_path, query_context::QueryContext},
};
use me_chat::{
    BLACKLISTED_GROUP_GUIDS, GROUP_GUIDS,
    user::{Users, get_users},
};
use rusqlite::Connection;
use tabled::{Table, Tabled};

/// Find group chats made up *entirely* of known users (from users.json) with at
/// least N distinct members, so uncovered groups worth adding to the gap check
/// can be spotted. Groups with any handle that isn't in users.json are skipped.
#[derive(clap::Args)]
pub struct Args {
    /// Minimum number of distinct known members a group must have
    #[arg(long, default_value_t = 4)]
    min_members: usize,

    /// Also list groups whose group_id is already covered by the gap check
    #[arg(long)]
    include_covered: bool,

    /// Number of recent messages to preview per group (0 disables previews)
    #[arg(long, default_value_t = 15)]
    messages: usize,
}

#[derive(Tabled)]
struct GroupRow {
    group_id: String,
    members: usize,
    covered: &'static str,
    names: String,
}

pub fn run(args: Args) -> Result<()> {
    let users = get_users()?;
    let covered: HashSet<&str> = GROUP_GUIDS.iter().copied().collect();
    let blacklisted: HashSet<&str> = BLACKLISTED_GROUP_GUIDS.iter().copied().collect();

    let db_path = default_db_path();
    let conn = get_connection(&db_path).wrap_err("failed to connect to database")?;

    // Collect the distinct handle ids and chat ROWIDs of each group_id. Multiple
    // chat rows can share a group_id, so both are aggregated across all of them.
    let mut group_handles: HashMap<String, HashSet<String>> = HashMap::new();
    let mut group_chats: HashMap<String, BTreeSet<i32>> = HashMap::new();

    let mut stmt = conn
        .prepare(
            "SELECT c.group_id, c.ROWID, h.id
             FROM chat c
             JOIN chat_handle_join chj ON chj.chat_id = c.ROWID
             JOIN handle h ON h.ROWID = chj.handle_id
             WHERE c.group_id IS NOT NULL",
        )
        .wrap_err("failed to prepare group membership query")?;

    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i32>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .wrap_err("failed to query group membership")?;

    for row in rows {
        let (group_id, chat_id, handle) = row?;
        group_chats
            .entry(group_id.clone())
            .or_default()
            .insert(chat_id);
        group_handles.entry(group_id).or_default().insert(handle);
    }

    let mut groups: Vec<GroupRow> = group_handles
        .into_iter()
        .filter_map(|(group_id, handles)| {
            // Explicitly excluded, even if every member is known.
            if blacklisted.contains(group_id.as_str()) {
                return None;
            }

            // Every handle must map to a known user; bail on the first unknown.
            let mut names = BTreeSet::new();
            for handle in &handles {
                names.insert(users.id_mapping.get(handle)?.clone());
            }

            if names.len() < args.min_members {
                return None;
            }

            let is_covered = covered.contains(group_id.as_str());
            if is_covered && !args.include_covered {
                return None;
            }

            Some(GroupRow {
                members: names.len(),
                covered: if is_covered { "yes" } else { "no" },
                names: names.into_iter().collect::<Vec<_>>().join(", "),
                group_id,
            })
        })
        .collect();

    groups.sort_by(|a, b| {
        b.members
            .cmp(&a.members)
            .then_with(|| a.group_id.cmp(&b.group_id))
    });

    if groups.is_empty() {
        println!(
            "no {}group_ids with {}+ known members found",
            if args.include_covered {
                ""
            } else {
                "uncovered "
            },
            args.min_members
        );
        return Ok(());
    }

    let mut table = Table::new(&groups);
    table.with(tabled::settings::Style::modern());
    println!("{table}");
    println!("{} group(s)", groups.len());

    if args.messages > 0 {
        let handles = Handle::cache(&conn).wrap_err("failed to cache handles")?;
        let offset = get_offset();

        for group in &groups {
            println!("\n=== {} ({}) ===", group.group_id, group.names);
            if let Some(chat_ids) = group_chats.get(&group.group_id) {
                print_preview(&conn, &handles, &users, offset, chat_ids, args.messages)?;
            } else {
                println!("  (no chats found)");
            }
        }
    }

    Ok(())
}

/// Print the last `limit` text messages from the given chats, oldest first.
fn print_preview(
    conn: &Connection,
    handles: &HashMap<i32, String>,
    users: &Users,
    offset: i64,
    chat_ids: &BTreeSet<i32>,
    limit: usize,
) -> Result<()> {
    let context = QueryContext {
        selected_chat_ids: Some(chat_ids.clone()),
        ..Default::default()
    };

    let mut statement =
        DbMessage::stream_rows(conn, &context).wrap_err("failed to stream messages")?;
    let rows = statement
        .query_map([], DbMessage::from_row)
        .wrap_err("failed to query messages")?
        .filter_map(Result::ok);

    // Messages stream oldest-first, so keep a sliding window of the last `limit`.
    let mut recent: VecDeque<String> = VecDeque::with_capacity(limit);
    let mut seen: HashSet<i32> = HashSet::new();

    for mut raw in rows {
        // A message shared across chat rows of the same group can appear twice.
        if !seen.insert(raw.rowid) {
            continue;
        }

        if let Ok(body) = raw.parse_body(conn) {
            raw.apply_body(body);
        }

        // Strip the attachment placeholder (U+FFFC) so attachment-only messages
        // are skipped and the preview favors messages with real text.
        let text = raw
            .text
            .as_deref()
            .unwrap_or_default()
            .replace('\u{fffc}', "")
            .replace('\n', " ");
        let text = text.trim();
        if text.is_empty() {
            continue;
        }

        let sender = if raw.is_from_me {
            "me".to_string()
        } else {
            raw.handle_id
                .and_then(|id| handles.get(&id))
                .map(|id| {
                    users
                        .id_mapping
                        .get(id)
                        .cloned()
                        .unwrap_or_else(|| id.clone())
                })
                .unwrap_or_else(|| "?".to_string())
        };

        let when = raw
            .date(offset)
            .map(|d| d.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|_| "?".to_string());

        if recent.len() == limit {
            recent.pop_front();
        }
        recent.push_back(format!("  {when}  {sender:>8}: {}", truncate(text, 90)));
    }

    if recent.is_empty() {
        println!("  (no text messages)");
    } else {
        for line in recent {
            println!("{line}");
        }
    }

    Ok(())
}

fn truncate(text: &str, max_chars: usize) -> String {
    if text.chars().count() > max_chars {
        let truncated: String = text.chars().take(max_chars.saturating_sub(1)).collect();
        format!("{truncated}…")
    } else {
        text.to_string()
    }
}
