use chrono::{DateTime, Timelike};
use chrono_tz::{America::New_York, Tz};
use color_eyre::eyre::{self, Context};
use crabstep::TypedStreamDeserializer;
use regex::Regex;
use std::{collections::HashSet, sync::LazyLock};

use crate::typedstream::{as_nsdictionary, as_nsstring, as_signed_integer};

static ME_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^\s*(not\s+)?me\W*$").unwrap());
static URL_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)https?://").unwrap());
static WORDLE_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^Wordle \d+ \d").unwrap());
// https://unicode.org/reports/tr51/#EBNF_and_Regex https://github.com/BurntSushi/ripgrep/discussions/1623#discussioncomment-28827
static EMOJI_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(\p{RI}\p{RI}|\p{Emoji}(\p{EMod}|\x{FE0F}\x{20E3}?|[\x{E0020}-\x{E007E}]+\x{E007F})?(\x{200D}\p{Emoji}(\p{EMod}|\x{FE0F}\x{20E3}?|[\x{E0020}-\x{E007E}]+\x{E007F})?)*)+$").unwrap()
});
static MEABLE_META_KEYS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    HashSet::from([
        "__kIMFileTransferGUIDAttributeName",
        "__kIMInlineMediaHeightAttributeName",
        "__kIMFilenameAttributeName",
        "__kIMInlineMediaWidthAttributeName",
        "__kIMLinkAttributeName",
        "__kIMLinkIsRichLinkAttributeName",
        // "__kIMDataDetectedAttributeName",
        "IMAudioTranscription",
        // TODO: figure out when this is clickable "__kIMCalendarEventAttributeName",
    ])
});

#[allow(dead_code)]
#[derive(Debug)]
pub struct Message {
    pub guid: String,
    pub id: String,
    pub date: DateTime<chrono_tz::Tz>,
    pub text: String,
    pub thread_originator_guid: Option<String>,
    pub kind: MessageKind,
    pub attributes: Option<HashSet<String>>,
}

#[derive(Debug)]
pub enum MessageKind {
    Normal,
    Me,
    NotMe,
    Meable,
    Spark,
    SparkCheat,
}

#[derive(Debug)]
pub struct MessageRow {
    pub guid: String,
    pub coredata_ns: i64,
    pub id: String,
    pub text: Option<String>,
    pub attributed_body: Option<Vec<u8>>,
    pub has_attachment: bool,
    pub balloon_bundle_id: Option<String>,
    pub thread_originator_guid: Option<String>,
}

impl Message {
    pub fn from_row(row: MessageRow) -> color_eyre::Result<Self> {
        let date = from_coredata_ns(row.coredata_ns);

        let body = row
            .attributed_body
            .as_ref()
            .map(|b| parse_attributed_body(b))
            .transpose()?
            .unwrap_or_else(|| AttributedBody {
                text: row.text.unwrap_or_default(),
                attributes: None,
            });

        let kind = if let Some(spark_type) = is_spark(date, &body.text) {
            match spark_type {
                SparkType::Spark => MessageKind::Spark,
                SparkType::SparkCheat => MessageKind::SparkCheat,
            }
        } else {
            let meable = body
                .attributes
                .as_ref()
                .map(|a| MEABLE_META_KEYS.iter().any(|k| a.contains(*k)))
                .unwrap_or(false)
                || (row.has_attachment
                    && (row.balloon_bundle_id.is_none()
                        || !row
                            .balloon_bundle_id
                            .as_ref()
                            .unwrap()
                            .contains("gamepigeon")))
                || URL_REGEX.is_match(&body.text)
                || WORDLE_REGEX.is_match(&body.text)
                || is_emoji_only(&body.text);

            if meable {
                MessageKind::Meable
            } else {
                let (me, not_me) = if let Some(captures) = ME_REGEX.captures(&body.text) {
                    if captures.get(1).is_some() {
                        (false, true)
                    } else {
                        (true, false)
                    }
                } else {
                    (false, false)
                };

                if me {
                    MessageKind::Me
                } else if not_me {
                    MessageKind::NotMe
                } else {
                    MessageKind::Normal
                }
            }
        };

        Ok(Message {
            guid: row.guid,
            id: row.id,
            date,
            text: body.text,
            thread_originator_guid: row.thread_originator_guid,
            kind,
            attributes: body.attributes,
        })
    }
}

enum SparkType {
    Spark,
    SparkCheat,
}

fn is_spark(date: DateTime<Tz>, text: &str) -> Option<SparkType> {
    if date.hour() == 16 && date.minute() == 20 && text.to_lowercase().trim() == "spark" {
        if date.timestamp_subsec_nanos() == 0 {
            Some(SparkType::SparkCheat)
        } else {
            Some(SparkType::Spark)
        }
    } else {
        None
    }
}

fn from_coredata_ns(coredata_ns: i64) -> DateTime<chrono_tz::Tz> {
    const CORE_DATA_EPOCH: i64 = 978_307_200;
    DateTime::from_timestamp_nanos(coredata_ns + CORE_DATA_EPOCH * 1_000_000_000)
        .with_timezone(&New_York)
}

fn is_emoji_only(text: &str) -> bool {
    !text
        .chars()
        .all(|c| c == '*' || c == '#' || c.is_ascii_digit())
        && EMOJI_REGEX.is_match(text)
}

#[derive(Debug)]
struct AttributedBody {
    text: String,
    attributes: Option<HashSet<String>>,
}

// TODO: parse attributedBody into its own struct so we can extract more information from it
fn parse_attributed_body(body: &[u8]) -> color_eyre::Result<AttributedBody> {
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

    let mut attributes: HashSet<String> = HashSet::new();

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

                attributes.insert(key);

                let _value = dict
                    .next()
                    .ok_or_else(|| eyre::eyre!("expected value for key in dictionary"))?;
            }
        }
    }

    Ok(AttributedBody {
        text,
        attributes: Some(attributes),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use color_eyre::eyre::Result;

    static ROWS: LazyLock<Vec<serde_json::Value>> = LazyLock::new(|| {
        const TEST_FILE_PATH: &str = "tests/messages.json";
        let file = std::fs::File::open(TEST_FILE_PATH)
            .unwrap_or_else(|_| panic!("failed to open {}", TEST_FILE_PATH));
        serde_json::from_reader(file)
            .unwrap_or_else(|_| panic!("failed to parse {}", TEST_FILE_PATH))
    });

    #[test]
    fn test_meable() -> Result<()> {
        assert!(matches!(
            parse_test_messasge("send_single_emoji")?.kind,
            MessageKind::Meable
        ));
        assert!(matches!(
            parse_test_messasge("recv_single_emoji")?.kind,
            MessageKind::Meable
        ));
        assert!(matches!(
            parse_test_messasge("send_double_emoji")?.kind,
            MessageKind::Meable
        ));
        assert!(matches!(
            parse_test_messasge("recv_double_emoji")?.kind,
            MessageKind::Meable
        ));
        assert!(matches!(
            parse_test_messasge("send_normal")?.kind,
            MessageKind::Normal
        ));
        assert!(matches!(
            parse_test_messasge("recv_normal")?.kind,
            MessageKind::Normal
        ));

        Ok(())
    }

    #[test]
    fn test_parse_attributed_body() -> Result<()> {
        let row = get_msg_json("recv_single_emoji").unwrap();

        let attributed_body = row
            .get("attributedBody")
            .and_then(|v| v.as_str())
            .map(|s| s.strip_prefix("0x").unwrap_or(s))
            .map(hex::decode)
            .transpose()?
            .unwrap();

        let body = parse_attributed_body(&attributed_body)?;

        println!("{:?}", body);

        assert_eq!(body.text, "😭");
        assert_eq!(
            body.attributes.unwrap(),
            HashSet::from(["__kIMMessagePartAttributeName".to_string()])
        );
        Ok(())
    }

    #[test]
    fn test_emoji_regex() {
        assert!(is_emoji_only("😂"));
        assert!(!is_emoji_only("ascii"));
        assert!(!is_emoji_only("😂ascii"));
        assert!(!is_emoji_only("0"));
        assert!(!is_emoji_only("***"));
        assert!(!is_emoji_only("###"));
    }

    fn get_msg_json(guid: &str) -> Option<&'static serde_json::Value> {
        ROWS.iter().find(|row| {
            row.get("guid")
                .and_then(|v| v.as_str())
                .map(|s| s == guid)
                .unwrap_or(false)
        })
    }

    fn parse_test_messasge(guid: &str) -> Result<Message> {
        if let Some(row) = get_msg_json(guid) {
            let coredata_ns = row.get("date").and_then(|v| v.as_i64()).unwrap();
            let id = row
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap();
            let text = row
                .get("text")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let attributed_body = row
                .get("attributedBody")
                .and_then(|v| v.as_str())
                .map(|s| s.strip_prefix("0x").unwrap_or(s))
                .map(hex::decode)
                .transpose()?;
            let has_attachment = row
                .get("has_attachment")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let balloon_bundle_id = row
                .get("balloon_bundle_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let thread_originator_guid = row
                .get("thread_originator_guid")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let msg = Message::from_row(MessageRow {
                guid: guid.to_string(),
                coredata_ns,
                id,
                text,
                attributed_body,
                has_attachment,
                balloon_bundle_id,
                thread_originator_guid,
            })?;

            println!("{:?}", msg);

            Ok(msg)
        } else {
            Err(eyre::eyre!(
                "message with guid {} not found in test data",
                guid
            ))
        }
    }
}
