use std::time::Duration;

use chrono::{DateTime, Utc};
use chrono_tz::America::New_York;
use color_eyre::eyre::{Result, eyre};
use futures::StreamExt;
use gap_check::{
    db::get_db_query,
    message::{MeableMessage, MessageKind},
    user::get_users,
};
use indicatif::{ProgressBar, ProgressStyle};
use tabled::{Table, Tabled};

const MAX_ME_COUNT: usize = 3;
const MEABLE_TIMEOUT_COUNT: usize = 20;
const SELF_ME_DELAY: Duration = Duration::from_secs(30);
const BACKUP_PATH_FORMAT: &str = "./chat.db.since%Y%m%d%H%M.bak";

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    dotenvy::dotenv().ok();

    let mut users = get_users()?;

    let mut meable_msgs: Vec<MeableMessage> = Vec::new();
    let mut possible_mes = 0;
    let mut last_spark = DateTime::from_timestamp_nanos(0).date_naive();

    let now = Utc::now().with_timezone(&New_York);
    let mut first_message_date = now;

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {human_pos} messages ({per_sec})")
            .unwrap(),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    let query = get_db_query().await?;
    let mut stream = query.as_stream();

    while let Some(msg) = stream.next().await.transpose()? {
        pb.inc(1);

        meable_msgs.iter_mut().for_each(|m| m.msgs_since += 1);

        let sender = users
            .id_mapping
            .get(&msg.id)
            .ok_or_else(|| eyre!("unknown user id: {}", msg.id))?;
        let user = users
            .by_name
            .get_mut(sender)
            .ok_or_else(|| eyre!("user not found: {}", sender))?;

        first_message_date = first_message_date.min(msg.date);

        // println!("{:?}", msg);
        // if msg.text.to_lowercase().contains("spark") {
        //     println!("{:?}", msg);
        // }

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
                        .ok_or_else(|| eyre!("unknown user id: {}", meable.msg.id))?;

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

                if let Some(ref thread_originator_guid) = msg.thread_originator_guid {
                    // me is a reply
                    if let Some(meable) = meable_msgs.iter_mut().find(|meable| {
                        &meable.msg.guid == thread_originator_guid
                            || meable.msg.thread_originator_guid.as_ref()
                                == Some(thread_originator_guid)
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

    query.drop().await;

    let backup_path = first_message_date.format(BACKUP_PATH_FORMAT).to_string();
    std::fs::copy(std::env::var("CHAT_DB_PATH").unwrap(), backup_path)?;

    Ok(())
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
