mod message;
mod typedstream;
mod user;

use chrono::{DateTime, Duration, Utc};
use chrono_tz::America::New_York;
use color_eyre::Result;
use indicatif::{ProgressIterator, ProgressStyle};
use sqlx::Row;
use sqlx::sqlite::SqlitePool;
use tabled::{Table, Tabled};

use crate::{
    message::{Meable, Message},
    user::get_users,
};

const MAX_ME_COUNT: usize = 3;
const MEABLE_TIMEOUT: Duration = Duration::hours(24);
const SELF_ME_DELAY: Duration = Duration::seconds(30);

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    dotenvy::dotenv().ok();

    let chat_db_path =
        std::env::var("CHAT_DB_PATH").expect("CHAT_DB_PATH environment variable not set");

    let fallback_id =
        std::env::var("FALLBACK_ID").expect("FALLBACK_ID environment variable not set");

    let mut users = get_users()?;

    let pool = SqlitePool::connect(&format!("sqlite://{}?mode=ro", chat_db_path)).await?;

    let mut meable_msgs: Vec<Meable> = Vec::new();
    let mut possible_mes = 0;
    let mut last_spark = DateTime::from_timestamp_nanos(0).date_naive();

    let now = Utc::now().with_timezone(&New_York);
    let mut first_message_date = now;

    let query = tokio::fs::read_to_string("messages_query.sql").await?;
    let rows = sqlx::query(&query).fetch_all(&pool).await?;

    println!("fetched {} messages", rows.len());

    for row in rows.iter().progress_with_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>7}/{len:7} ({per_sec})").unwrap()) {
        let date_ns: i64 = row.try_get("date")?;
        let id = row
            .try_get::<Option<String>, _>("id")?
            .unwrap_or_else(|| fallback_id.clone());
        let text: Option<String> = row.try_get("text")?;
        let attributed_body: Option<&[u8]> = row.try_get("attributedBody")?;
        let has_attachment: i32 = row.try_get("has_attachment")?;
        let balloon_bundle_id: Option<String> = row.try_get("balloon_bundle_id")?;

        let msg = Message::from_row(
            date_ns,
            id.clone(),
            text,
            attributed_body,
            has_attachment,
            balloon_bundle_id,
        )?;

        let user_name = users.id_mapping.get(&id).unwrap();
        let user = users.by_name.get_mut(user_name).unwrap();

        match msg {
            Message::Me(ref m) | Message::NotMe(ref m) | Message::Meable(Meable { message: ref m, ..}) | Message::Spark(ref m) | Message::SparkCheat(ref m) | Message::Normal(ref m) => {
                first_message_date = first_message_date.min(m.date);

                if m.text.to_lowercase().contains("spark") {
                    // println!("{:?}", m);
                }
            }
        }

        match msg {
            Message::Spark(m) => {
                if m.date.date_naive() > last_spark {
                    user.sparks += 1;
                    last_spark = m.date.date_naive();
                }
                continue;
            },
            Message::SparkCheat(_) => {
                user.spark_cheats += 1;
                continue;
            },
            Message::Meable(m) => {
                user.meable_message_count += 1;
                meable_msgs.push(m);
                possible_mes += MAX_ME_COUNT;
                continue;
            },
            Message::Me(ref m) | Message::NotMe(ref m) => {
                meable_msgs.retain(|meable| m.date < meable.message.date + MEABLE_TIMEOUT);

                meable_msgs.sort_by_key(|meable| meable.message.date);

                for meable in &mut meable_msgs {
                    if meable.mes.contains(&id) {
                        continue;
                    }

                    if meable.mes.is_empty()
                        && id == meable.message.id
                        && m.date < meable.message.date + SELF_ME_DELAY
                    {
                        continue;
                    }

                    meable.mes.push(id.clone());

                    if id == meable.message.id {
                        user.own_mes_count += 1;
                    }

                    match msg {
                        Message::Me(_) => user.mes += 1,
                        Message::NotMe(_) => user.not_mes += 1,
                        _ => unreachable!(),
                    }

                    break;
                }

                meable_msgs.retain(|meable| meable.mes.len() < MAX_ME_COUNT);
            },
            Message::Normal(_) => {},
        }
    }

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
