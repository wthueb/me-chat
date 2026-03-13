use std::{collections::HashMap, sync::LazyLock};

use chrono::{DateTime, Timelike as _};
use chrono_tz::{America::New_York, Tz};
use color_eyre::eyre::{Result, eyre};
use imessage_database::{
    message_types::{
        app::AppMessage,
        text_effects::TextEffect,
        variants::{BalloonProvider as _, CustomBalloon, Variant},
    },
    tables::{
        attachment::Attachment,
        messages::{Message as DbMessage, models::BubbleComponent},
        table::ME,
    },
    util::plist::parse_ns_keyed_archiver,
};
use regex::Regex;
use rusqlite::Connection;

#[derive(Debug)]
pub struct Message {
    pub guid: String,
    pub id: String,
    pub date: DateTime<Tz>,
    pub text: String,
    pub kind: MessageKind,
    pub raw: DbMessage,
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.guid == other.guid
    }
}

impl Eq for Message {}

#[derive(Debug)]
pub enum MessageKind {
    Ignore,
    Normal,
    Me,
    NotMe,
    Meable(Vec<MeableType>),
    Spark,
    SparkCheat,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum MeableType {
    Attachment,
    Url,
    Emoji,
    Wordle,
    Handwriting,
    DigitalTouch,
    Poll,
    AppBalloon,
    ApplePay,
}

impl MessageKind {
    fn detect(raw: &DbMessage, conn: &Connection) -> Result<Self> {
        let variant = raw.variant();

        if matches!(variant, Variant::Tapback(..)) || raw.is_announcement() {
            return Ok(MessageKind::Ignore);
        }

        let meables = MeableType::detect(raw, conn)?;
        if !meables.is_empty() {
            return Ok(MessageKind::Meable(meables));
        }

        if let Some(text) = &raw.text {
            let date = from_coredata_ns(raw.date);

            if let Some(spark_type) = SparkType::detect(date, text) {
                return Ok(match spark_type {
                    SparkType::Spark => MessageKind::Spark,
                    SparkType::SparkCheat => MessageKind::SparkCheat,
                });
            }

            static ME_REGEX: LazyLock<Regex> =
                LazyLock::new(|| Regex::new(r"(?i)^\s*(not\s+)?me\W*$").unwrap());

            if let Some(captures) = ME_REGEX.captures(text) {
                if captures.get(1).is_some() {
                    return Ok(MessageKind::NotMe);
                }

                return Ok(MessageKind::Me);
            }
        }

        Ok(MessageKind::Normal)
    }
}

impl MeableType {
    fn detect(raw: &DbMessage, conn: &Connection) -> Result<Vec<Self>> {
        if raw.is_announcement() {
            return Ok(vec![]);
        }

        let variant = raw.variant();

        match variant {
            Variant::App(ref balloon) => match balloon {
                CustomBalloon::URL => return Ok(vec![Self::Url]),
                CustomBalloon::Handwriting => return Ok(vec![Self::Handwriting]),
                CustomBalloon::DigitalTouch => return Ok(vec![Self::DigitalTouch]),
                CustomBalloon::ApplePay => return Ok(vec![Self::ApplePay]),
                CustomBalloon::Polls => return Ok(vec![]),
                CustomBalloon::Application(bundle_id) => {
                    if *bundle_id == "com.gamerdelights.gamepigeon.ext"
                        || *bundle_id == "com.nearfuturespecialists.imessagepoll.MessagesExtension"
                    {
                        return Ok(vec![]);
                    } else {
                        return Ok(vec![Self::AppBalloon]);
                    }
                }
                CustomBalloon::Slideshow => {
                    let payload = raw
                        .payload_data(conn)
                        .ok_or_else(|| eyre!("slideshow balloon missing payload"))?;
                    let parsed = parse_ns_keyed_archiver(&payload)
                        .map_err(|e| eyre!("failed to parse slideshow payload: {e}"))?;
                    let balloon = AppMessage::from_map(&parsed)
                        .map_err(|e| eyre!("failed to parse slideshow app message: {e}"))?;
                    println!("got slideshow with balloon: {:#?}", balloon);
                    return Ok(vec![Self::Attachment]);
                }
                CustomBalloon::Fitness | CustomBalloon::CheckIn | CustomBalloon::FindMy => {
                    unimplemented!("{variant:?} {raw:#?}")
                }
            },
            Variant::Tapback(..) | Variant::PollUpdate | Variant::Vote => return Ok(vec![]),
            Variant::Normal | Variant::Edited | Variant::SharePlay | Variant::Unknown(_) => {}
        }

        let mut meables = Vec::new();

        for component in raw.components.iter() {
            match component {
                BubbleComponent::Text(attributes) => {
                    let urls = attributes
                        .iter()
                        .flat_map(|a| &a.effects)
                        .filter_map(|e| match e {
                            TextEffect::Link(url) => Some(url),
                            _ => None,
                        })
                        .filter(|url| !url.starts_with("tel:"))
                        .collect::<Vec<_>>();

                    meables.extend(std::iter::repeat_n(Self::Url, urls.len()));
                }
                BubbleComponent::App
                | BubbleComponent::Attachment(..)
                | BubbleComponent::Retracted => {}
            }
        }

        if let Some(text) = &raw.text {
            if is_emoji_only(text) {
                meables.push(Self::Emoji);
            }

            static WORDLE_REGEX: LazyLock<Regex> =
                LazyLock::new(|| Regex::new(r"^Wordle \d+ \d").unwrap());
            if WORDLE_REGEX.is_match(text) {
                return Ok(vec![Self::Wordle]);
            }
        }

        let attachments = Attachment::from_message(conn, raw)
            .map_err(|e| eyre!("failed to get attachments for message: {e}"))?
            .into_iter()
            .filter(|a| a.hide_attachment == 0 && !a.is_sticker)
            .collect::<Vec<_>>();

        meables.extend(std::iter::repeat_n(Self::Attachment, attachments.len()));

        Ok(meables)
    }
}

fn is_emoji_only(text: &str) -> bool {
    // https://unicode.org/reports/tr51/#EBNF_and_Regex
    // https://github.com/BurntSushi/ripgrep/discussions/1623#discussioncomment-28827
    static EMOJI_REGEX: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^(\p{RI}\p{RI}|\p{Emoji}(\p{EMod}|\x{FE0F}\x{20E3}?|[\x{E0020}-\x{E007E}]+\x{E007F})?(\x{200D}\p{Emoji}(\p{EMod}|\x{FE0F}\x{20E3}?|[\x{E0020}-\x{E007E}]+\x{E007F})?)*)+$").unwrap()
    });

    let no_whitespace: String = text.chars().filter(|c| !c.is_whitespace()).collect();

    !no_whitespace
        .chars()
        .all(|c| c == '*' || c == '#' || c.is_ascii_digit())
        && EMOJI_REGEX.is_match(&no_whitespace)
}

impl Message {
    pub fn from_raw(
        raw: DbMessage,
        conn: &Connection,
        handles: &HashMap<i32, String>,
    ) -> Result<Self> {
        let kind = MessageKind::detect(&raw, conn)?;

        static FALLBACK_ID: LazyLock<String> = LazyLock::new(|| {
            std::env::var("FALLBACK_ID").expect("FALLBACK_ID environment variable not set")
        });

        let id = handles
            .get(&raw.handle_id.unwrap())
            .filter(|handle| *handle != ME)
            .cloned()
            .unwrap_or_else(|| FALLBACK_ID.clone());

        Ok(Message {
            guid: raw.guid.clone(),
            id,
            date: from_coredata_ns(raw.date),
            text: raw.text.clone().unwrap_or_default(),
            kind,
            raw,
        })
    }
}

fn from_coredata_ns(coredata_ns: i64) -> DateTime<Tz> {
    const CORE_DATA_EPOCH: i64 = 978_307_200;
    DateTime::from_timestamp_nanos(coredata_ns + CORE_DATA_EPOCH * 1_000_000_000)
        .with_timezone(&New_York)
}

enum SparkType {
    Spark,
    SparkCheat,
}

impl SparkType {
    fn detect(date: DateTime<Tz>, text: &str) -> Option<Self> {
        if date.hour() == 16 && date.minute() == 20 && text.to_lowercase().trim() == "spark" {
            if date.timestamp_subsec_nanos() == 0 {
                Some(Self::SparkCheat)
            } else {
                Some(Self::Spark)
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_emoji_only() {
        assert!(is_emoji_only("😀"));
        assert!(is_emoji_only("😀😀"));
        assert!(is_emoji_only("😀👩‍👩‍👧‍👦"));
        assert!(!is_emoji_only("Hello 😀"));
        assert!(!is_emoji_only("😀 Hello"));
        assert!(!is_emoji_only("Hello"));
        assert!(!is_emoji_only("123"));
        assert!(!is_emoji_only("*#"));
    }
}
