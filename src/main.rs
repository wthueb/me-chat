mod message;
mod typedstream;
mod user;

use std::time::Duration;

use arrayvec::ArrayVec;
use chrono::{DateTime, Utc};
use chrono_tz::America::New_York;
use color_eyre::{Result, eyre};
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use sqlx::sqlite::SqlitePool;
use tabled::{Table, Tabled};

use crate::{
    message::{Message, MessageKind, MessageRow},
    user::get_users,
};

const MAX_ME_COUNT: usize = 3;
const MEABLE_TIMEOUT_COUNT: usize = 20;
const SELF_ME_DELAY: Duration = Duration::from_secs(30);
const BACKUP_PATH_FORMAT: &str = "./chat.db.since%Y%m%d%H%M.bak";

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    dotenvy::dotenv().ok();

    let chat_db_path =
        std::env::var("CHAT_DB_PATH").expect("CHAT_DB_PATH environment variable not set");

    let fallback_id =
        std::env::var("FALLBACK_ID").expect("FALLBACK_ID environment variable not set");

    let mut users = get_users()?;

    let pool = SqlitePool::connect(&format!("sqlite:{}?mode=ro", chat_db_path)).await?;

    let mut meable_msgs: Vec<MeableMessage> = Vec::new();
    let mut possible_mes = 0;
    let mut last_spark = DateTime::from_timestamp_nanos(0).date_naive();

    let now = Utc::now().with_timezone(&New_York);
    let mut first_message_date = now;

    let mut rows = sqlx::query!(
        r#"
        select
            message.guid,
            message.date,
            handle.id,
            message.text,
            message.attributedBody,
            message.cache_has_attachments as has_attachment,
            message.balloon_bundle_id,
            message.thread_originator_guid
        from
            message
        inner join chat_message_join on
            message.ROWID = chat_message_join.message_id
        inner join chat on
            chat_message_join.chat_id = chat.ROWID
        left join handle on
            message.handle_id = handle.ROWID
        where
            chat.group_id in (
                '647850EE-DD0A-4875-9306-BC6A8E560F16',
                'C1C65CF7-828E-41EF-91A8-179E80849987',
                '46324139453632322D394641332D343032442D394433452D413341413544414335313843'
            )
            and message.associated_message_guid is null -- exclude reactions and stickers
        order by
            date asc
        "#
    )
    .fetch(&pool);

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {human_pos} messages ({per_sec})")
            .unwrap(),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    while let Some(row) = rows.next().await.transpose()? {
        pb.inc(1);

        let msg = Message::from_row(MessageRow {
            guid: row.guid,
            coredata_ns: row.date.unwrap(),
            id: row.id.unwrap_or_else(|| fallback_id.clone()),
            text: row.text,
            attributed_body: row.attributedBody,
            has_attachment: row.has_attachment.unwrap_or(0) != 0,
            balloon_bundle_id: row.balloon_bundle_id,
            thread_originator_guid: row.thread_originator_guid,
        })?;

        meable_msgs.iter_mut().for_each(|m| m.msgs_since += 1);

        let sender = users
            .id_mapping
            .get(&msg.id)
            .ok_or_else(|| eyre::eyre!("unknown user id: {}", msg.id))?;
        let user = users
            .by_name
            .get_mut(sender)
            .ok_or_else(|| eyre::eyre!("user not found: {}", sender))?;

        first_message_date = first_message_date.min(msg.date);

        // println!("{:?}", msg);
        if msg.text.to_lowercase().contains("spark") {
            // println!("{:?}", msg);
        }

        match msg.kind {
            MessageKind::Spark => {
                if msg.date.date_naive() > last_spark {
                    user.sparks += 1;
                    last_spark = msg.date.date_naive();
                }
                continue;
            }
            MessageKind::SparkCheat => {
                user.spark_cheats += 1;
                continue;
            }
            MessageKind::Meable => {
                user.meable_message_count += 1;
                // println!("{:?}", msg);
                meable_msgs.push(msg.try_into().unwrap());
                possible_mes += MAX_ME_COUNT;
                continue;
            }
            MessageKind::Me | MessageKind::NotMe => {
                let mut try_to_me = |meable: &mut MeableMessage| -> Result<bool> {
                    let meable_sender = users
                        .id_mapping
                        .get(&meable.msg.id)
                        .ok_or_else(|| eyre::eyre!("unknown user id: {}", meable.msg.id))?;

                    match msg.kind {
                        MessageKind::Me | MessageKind::NotMe => {}
                        _ => return Ok(false),
                    }

                    // already me'd
                    if meable.mes.contains(sender) {
                        return Ok(false);
                    }

                    // can't self me within the delay period unless it's already been me'd
                    if meable.mes.is_empty()
                        && sender == meable_sender
                        && msg.date < meable.msg.date + SELF_ME_DELAY
                    {
                        return Ok(false);
                    }

                    meable.mes.push(sender.to_string());

                    if sender == meable_sender {
                        user.own_mes_count += 1;
                    }

                    match msg.kind {
                        MessageKind::Me => user.mes += 1,
                        MessageKind::NotMe => user.not_mes += 1,
                        _ => unreachable!(),
                    }

                    Ok(true)
                };

                let mut med = false;

                if let Some(thread_originator_guid) = msg.thread_originator_guid {
                    // me is a reply
                    if let Some(meable) = meable_msgs.iter_mut().find(|meable| {
                        meable.msg.guid == thread_originator_guid
                            || meable.msg.thread_originator_guid
                                == Some(thread_originator_guid.clone())
                    }) {
                        med = try_to_me(meable)?;
                    }
                } else {
                    for meable in meable_msgs
                        .iter_mut()
                        .filter(|meable| meable.msgs_since - 1 <= MEABLE_TIMEOUT_COUNT)
                    {
                        if try_to_me(meable)? {
                            med = true;
                            break;
                        }
                    }
                }

                if med {
                    meable_msgs.retain(|meable| meable.mes.len() < MAX_ME_COUNT);
                }
            }
            MessageKind::Normal => {}
        }
    }

    pb.finish();

    // for meable in &meable_msgs {
    //     println!("{:?}", meable);
    // }

    println!("gap check since: {}", first_message_date);

    let mut stats: Vec<UserStats> = users
        .by_name
        .values()
        .map(|user| UserStats {
            user: user.name.clone(),
            mes: user.mes,
            not_mes: user.not_mes,
            total: user.total(),
            sparks: user.sparks,
            spark_cheats: user.spark_cheats,
            meable_messages: user.meable_message_count,
            own_mes: user.own_mes_count,
            own_mes_percent: user.own_mes_percent(),
        })
        .collect();

    stats.sort_by(|a, b| b.total.cmp(&a.total));

    let mut table = Table::new(stats);
    table.with(tabled::settings::Style::modern());
    println!("{}", table);

    let total_mes: usize = users.by_name.values().map(|u| u.total()).sum();
    println!("{}/{}", total_mes, possible_mes);

    pool.close().await;

    let backup_path = first_message_date.format(BACKUP_PATH_FORMAT).to_string();
    std::fs::copy(&chat_db_path, backup_path)?;

    Ok(())
}

#[derive(Debug)]
struct MeableMessage {
    msg: Message,
    mes: ArrayVec<String, 3>,
    msgs_since: usize,
}

impl TryFrom<Message> for MeableMessage {
    type Error = eyre::Report;

    fn try_from(msg: Message) -> Result<MeableMessage> {
        match msg.kind {
            MessageKind::Meable => Ok(MeableMessage {
                msg,
                mes: ArrayVec::new(),
                msgs_since: 0,
            }),
            _ => Err(eyre::eyre!("message is not meable")),
        }
    }
}

#[derive(Tabled)]
struct UserStats {
    user: String,
    mes: usize,
    not_mes: usize,
    total: usize,
    sparks: usize,
    spark_cheats: usize,
    meable_messages: usize,
    own_mes: usize,
    #[tabled(rename = "own mes %")]
    own_mes_percent: String,
}
