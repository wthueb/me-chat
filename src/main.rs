use std::{collections::HashSet, ops::Deref, sync::Arc, time::Duration};

use arrayvec::ArrayVec;
use chrono::{DateTime, Utc};
use chrono_tz::America::New_York;
use color_eyre::eyre::{Result, eyre};
use gap_check::{
    db::Db,
    message::{MeableType, Message, MessageKind},
    user::get_users,
};
use imessage_database::{tables::table::get_connection, util::dirs::default_db_path};
use indicatif::{ProgressBar, ProgressStyle};
use tabled::{Table, Tabled};

const MAX_ME_COUNT: usize = 3;
const MEABLE_TIMEOUT_COUNT: usize = 20;
const SELF_ME_DELAY: Duration = Duration::from_secs(30);
const BACKUP_PATH_FORMAT: &str = "./chat.db.since%Y%m%d%H%M.bak";

fn main() -> Result<()> {
    color_eyre::install()?;
    dotenvy::dotenv().ok();

    let mut users = get_users()?;

    let mut meable_msgs: Vec<MeableMessage> = Vec::new();
    let mut possible_mes = 0;
    let mut last_spark = DateTime::from_timestamp_nanos(0).date_naive();

    let now = Utc::now().with_timezone(&New_York);
    let mut first_message_date = now;

    let db_path = default_db_path();

    let conn = get_connection(&db_path).map_err(|e| eyre!("failed to connect to database: {e}"))?;

    let group_guids = HashSet::from([
        "647850EE-DD0A-4875-9306-BC6A8E560F16",
        "C1C65CF7-828E-41EF-91A8-179E80849987",
        "46324139453632322D394641332D343032442D394433452D413341413544414335313843",
    ]);

    let db = Db::new(&conn, &group_guids)?;

    let total_messages = db.get_count()?;

    let pb = ProgressBar::new(total_messages.try_into()?);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{bar:40.cyan/blue} {pos}/{len} messages ({per_sec})")
            .unwrap(),
    );

    for msg in db.iter_messages()? {
        pb.inc(1);
        let msg = Arc::new(msg?);

        if matches!(msg.kind, MessageKind::Ignore) {
            continue;
        }

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
        //     println!("{:?} {}: {}", msg.date, sender, msg.text);
        // }

        match msg.kind {
            MessageKind::Spark => {
                if msg.date.date_naive() > last_spark {
                    user.sparks += 1;
                    last_spark = msg.date.date_naive();
                }
            }
            MessageKind::SparkCheat => {
                user.spark_cheats += 1;
            }
            MessageKind::Meable(_) => {
                user.meable_message_count += 1;
                // println!("{:?}", msg);
                meable_msgs.extend(MeableMessage::from_msg(Arc::clone(&msg))?);
                possible_mes += MAX_ME_COUNT;
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

                if let Some(ref thread_originator_guid) = msg.raw.thread_originator_guid {
                    // me is a reply
                    if let Some(meable) = meable_msgs.iter_mut().find(|meable| {
                        &meable.msg.guid == thread_originator_guid
                            || meable.msg.raw.thread_originator_guid.as_ref()
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
            MessageKind::Ignore => unreachable!(),
        }
    }

    pb.finish();

    // for meable in &meable_msgs {
    //     println!("{meable:?}");
    // }

    println!("gap check since: {first_message_date}");

    let mut stats = users
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
        .collect::<Vec<_>>();

    stats.sort_by(|a, b| b.total.cmp(&a.total));

    let mut table = Table::new(stats);
    table.with(tabled::settings::Style::modern());
    println!("{table}");

    let total_mes: usize = users.by_name.values().map(|u| u.total()).sum();
    println!("{total_mes}/{possible_mes}");

    let backup_path = first_message_date.format(BACKUP_PATH_FORMAT).to_string();
    std::fs::copy(db_path, backup_path)?;

    Ok(())
}

#[derive(Debug)]
pub struct MeableMessage {
    pub msg: Arc<Message>,
    pub kind: MeableType,
    pub mes: ArrayVec<String, 3>,
    pub msgs_since: usize,
}

impl MeableMessage {
    fn from_msg(msg: Arc<Message>) -> Result<Vec<Self>> {
        if let MessageKind::Meable(ref meables) = msg.kind {
            Ok(meables
                .iter()
                .map(|kind| MeableMessage {
                    msg: Arc::clone(&msg),
                    kind: *kind,
                    mes: ArrayVec::new(),
                    msgs_since: 0,
                })
                .collect())
        } else {
            Err(eyre!("message is not meable"))
        }
    }
}

impl Deref for MeableMessage {
    type Target = Message;

    fn deref(&self) -> &Self::Target {
        &self.msg
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
