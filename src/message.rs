use arrayvec::ArrayVec;
use chrono::{DateTime, Timelike};
use chrono_tz::America::New_York;
use color_eyre::eyre::{self, Context};
use crabstep::TypedStreamDeserializer;
use regex::Regex;
use std::{collections::HashSet, sync::LazyLock};

use crate::typedstream::{as_nsdictionary, as_nsstring, as_signed_integer};

const CORE_DATA_EPOCH: i64 = 978_307_200;
static ME_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^\s*(not\s+)?me\W*$").unwrap());
static URL_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)https?://").unwrap());
static WORDLE_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^Wordle \d+ \d").unwrap());
static MEABLE_META_KEYS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    HashSet::from([
        "__kIMFileTransferGUIDAttributeName",
        "__kIMInlineMediaHeightAttributeName",
        "__kIMFilenameAttributeName",
        "__kIMInlineMediaWidthAttributeName",
        "__kIMLinkAttributeName",
        "__kIMLinkIsRichLinkAttributeName",
        "__kIMDataDetectedAttributeName",
        "IMAudioTranscription",
        // TODO: figure out when this is clickable "__kIMCalendarEventAttributeName",
    ])
});

#[derive(Debug, Clone)]
pub struct MessageData {
    pub id: String,
    pub date: DateTime<chrono_tz::Tz>,
    pub text: String,
}

#[derive(Debug)]
pub struct Meable {
    pub message: MessageData,
    pub mes: ArrayVec<String, 3>,
}

#[derive(Debug)]
pub enum Message {
    Normal(MessageData),
    Me(MessageData),
    NotMe(MessageData),
    Meable(Meable),
    Spark(MessageData),
    SparkCheat(MessageData),
}

impl Message {
    pub fn from_row(
        coredata_ns: i64,
        id: String,
        text: Option<String>,
        attributed_body: Option<&[u8]>,
        has_attachment: i32,
        balloon_bundle_id: Option<String>,
    ) -> color_eyre::Result<Self> {
        let date = DateTime::from_timestamp_nanos(coredata_ns + CORE_DATA_EPOCH * 1_000_000_000)
            .with_timezone(&New_York);

        let (text, mut meable) = if let Some(body) = attributed_body {
            parse_attributed_body(body)?
        } else {
            (text.unwrap_or_default(), false)
        };

        if date.hour() == 16 && date.minute() == 20 && text.to_lowercase().trim() == "spark" {
            if date.timestamp_subsec_nanos() == 0 {
                return Ok(Message::SparkCheat(MessageData { id, date, text }));
            }
            return Ok(Message::Spark(MessageData { id, date, text }));
        }

        meable = meable
            || (has_attachment != 0
                && (balloon_bundle_id.is_none()
                    || !balloon_bundle_id.as_ref().unwrap().contains("gamepigeon")))
            || URL_REGEX.is_match(&text)
            || WORDLE_REGEX.is_match(&text);

        if meable {
            return Ok(Message::Meable(Meable {
                message: MessageData { id, date, text },
                mes: ArrayVec::new(),
            }));
        }

        let (me, not_me) = if let Some(captures) = ME_REGEX.captures(&text) {
            if captures.get(1).is_some() {
                (false, true)
            } else {
                (true, false)
            }
        } else {
            (false, false)
        };

        if me {
            return Ok(Message::Me(MessageData { id, date, text }));
        }

        if not_me {
            return Ok(Message::NotMe(MessageData { id, date, text }));
        }

        Ok(Message::Normal(MessageData { id, date, text }))
    }
}

fn parse_attributed_body(body: &[u8]) -> color_eyre::Result<(String, bool)> {
    let mut deserializer = TypedStreamDeserializer::new(body);
    let mut props = deserializer
        .iter_root()
        .wrap_err("failed to iterate root properties")?;

    // first prop is the string
    let text = props
        .next()
        .as_mut()
        .and_then(as_nsstring)
        .ok_or_else(|| eyre::eyre!("first property not a NSString"))?
        .to_string();

    for mut prop in props {
        if let Some(dict) = as_nsdictionary(&mut prop) {
            let num_items = dict
                .next()
                .as_ref()
                .and_then(as_signed_integer)
                .ok_or_else(|| eyre::eyre!("expected number of items in dictionary"))?;

            for _ in 0..num_items {
                let key = dict
                    .next()
                    .as_mut()
                    .and_then(as_nsstring)
                    .ok_or_else(|| eyre::eyre!("expected string key in dictionary"))?
                    .to_string();

                if MEABLE_META_KEYS.contains(key.as_str()) {
                    return Ok((text, true));
                }

                let _value = dict
                    .next()
                    .ok_or_else(|| eyre::eyre!("expected value for key in dictionary"))?;
            }
        }
    }

    Ok((text, false))
}

#[test]
fn test_url() -> color_eyre::Result<()> {
    let message = Message::from_row(
        0,
        "1".to_string(),
        Some("Check this out: https://example.com".to_string()),
        None,
        0,
        None,
    )?;

    assert!(matches!(message, Message::Meable(_)));

    Ok(())
}
