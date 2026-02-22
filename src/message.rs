use arrayvec::ArrayVec;
use chrono::{DateTime, Timelike};
use chrono_tz::{America::New_York, Tz};
use color_eyre::eyre::{self, Context, Result, eyre};
use crabstep::TypedStreamDeserializer;
use regex::Regex;
use std::{collections::HashSet, sync::LazyLock};

use crate::{
    db::MessageRow,
    typedstream::{as_nsdictionary, as_nsstring, as_signed_integer},
};

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

impl MessageKind {
    fn detect(row: &MessageRow, date: DateTime<Tz>, body: &AttributedBody) -> Self {
        match SparkType::detect(date, &body.text) {
            Some(SparkType::Spark) => return MessageKind::Spark,
            Some(SparkType::SparkCheat) => return MessageKind::SparkCheat,
            None => {}
        }

        if is_meable(row, body) {
            return MessageKind::Meable;
        }

        static ME_REGEX: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"(?i)^\s*(not\s+)?me\W*$").unwrap());

        if let Some(captures) = ME_REGEX.captures(&body.text) {
            if captures.get(1).is_some() {
                return MessageKind::NotMe;
            }

            return MessageKind::Me;
        }

        MessageKind::Normal
    }
}

impl TryFrom<MessageRow> for Message {
    type Error = eyre::Report;

    fn try_from(row: MessageRow) -> Result<Self> {
        Message::try_from(&row)
    }
}

impl TryFrom<&MessageRow> for Message {
    type Error = eyre::Report;

    fn try_from(row: &MessageRow) -> Result<Self> {
        let date = from_coredata_ns(
            row.date
                .ok_or_else(|| eyre!("missing date for message with guid {}", row.guid))?,
        );

        let body = row
            .attributed_body
            .as_ref()
            .map(|b| AttributedBody::parse(b))
            .transpose()
            .wrap_err("failed to parse attributed body")?
            .unwrap_or_else(|| AttributedBody {
                text: row.text.clone().unwrap_or_default(),
                attributes: None,
            });

        let kind = MessageKind::detect(row, date, &body);

        static FALLBACK_ID: LazyLock<String> = LazyLock::new(|| {
            std::env::var("FALLBACK_ID").expect("FALLBACK_ID environment variable not set")
        });

        Ok(Message {
            guid: row.guid.clone(),
            id: row.id.clone().unwrap_or_else(|| FALLBACK_ID.clone()),
            date,
            text: body.text,
            thread_originator_guid: row.thread_originator_guid.clone(),
            kind,
            attributes: body.attributes,
        })
    }
}

fn from_coredata_ns(coredata_ns: i64) -> DateTime<chrono_tz::Tz> {
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

fn is_meable(row: &MessageRow, body: &AttributedBody) -> bool {
    static WORDLE_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^Wordle \d+ \d").unwrap());
    static MEABLE_ATTRIBUTES: LazyLock<[HashSet<&'static str>; 5]> = LazyLock::new(|| {
        [
            HashSet::from(["__kIMFileTransferGUIDAttributeName"]), // images/videos/other files
            HashSet::from(["__kIMLinkAttributeName", "__kIMDataDetectedAttributeName"]), // links
            HashSet::from(["__kIMLinkAttributeName", "__kIMPhoneNumberAttributeName"]), // phone numbers
            HashSet::from(["__kIMLinkAttributeName", "__kIMAddressAttributeName"]),     // addresses
            HashSet::from([
                "__kIMBreadcrumbTextMarkerAttributeName",
                "__kIMBreadcrumbTextOptionFlags",
            ]), // sent via icloud breadcrumb?
        ]
    });

    if let Some(attributes) = &body.attributes {
        let attributes = attributes
            .iter()
            .map(|s| s.as_str())
            .collect::<HashSet<_>>();

        if MEABLE_ATTRIBUTES.iter().any(|a| a.is_subset(&attributes)) {
            return true;
        }
    }

    (row.has_attachment.unwrap_or(0) != 0
        && (row.balloon_bundle_id.is_none()
            || !row
                .balloon_bundle_id
                .as_ref()
                .unwrap()
                .contains("gamepigeon")))
        || WORDLE_REGEX.is_match(&body.text)
        || is_emoji_only(&body.text)
}

pub fn is_emoji_only(text: &str) -> bool {
    // https://unicode.org/reports/tr51/#EBNF_and_Regex
    // https://github.com/BurntSushi/ripgrep/discussions/1623#discussioncomment-28827
    static EMOJI_REGEX: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^(\p{RI}\p{RI}|\p{Emoji}(\p{EMod}|\x{FE0F}\x{20E3}?|[\x{E0020}-\x{E007E}]+\x{E007F})?(\x{200D}\p{Emoji}(\p{EMod}|\x{FE0F}\x{20E3}?|[\x{E0020}-\x{E007E}]+\x{E007F})?)*)+$").unwrap()
    });

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

impl AttributedBody {
    fn parse(data: &[u8]) -> Result<Self> {
        let mut deserializer = TypedStreamDeserializer::new(data);
        let mut props = deserializer
            .iter_root()
            .wrap_err("failed to iterate root properties")?;

        // first prop is the string
        let text = props
            .next()
            .as_mut()
            .and_then(as_nsstring)
            .ok_or_else(|| eyre!("first property not a NSString"))?
            .to_string();

        let mut attributes: HashSet<String> = HashSet::new();

        for mut prop in props {
            if let Some(dict) = as_nsdictionary(&mut prop) {
                let num_items = dict
                    .next()
                    .as_ref()
                    .and_then(as_signed_integer)
                    .ok_or_else(|| eyre!("expected number of items in dictionary"))?;

                for _ in 0..num_items {
                    let key = dict
                        .next()
                        .as_mut()
                        .and_then(as_nsstring)
                        .ok_or_else(|| eyre!("expected string key in dictionary"))?
                        .to_string();

                    attributes.insert(key);

                    let _value = dict
                        .next()
                        .ok_or_else(|| eyre!("expected value for key in dictionary"))?;
                }
            }
        }

        Ok(AttributedBody {
            text,
            attributes: Some(attributes),
        })
    }
}

#[derive(Debug)]
pub struct MeableMessage {
    pub msg: Message,
    pub mes: ArrayVec<String, 3>,
    pub msgs_since: usize,
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
            _ => Err(eyre!("message is not meable")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_meable() -> Result<()> {
        let file = TestFile::new("tests/messages.json")?;

        assert!(matches!(
            file.get_msg("send_single_emoji")?.kind,
            MessageKind::Meable
        ));
        assert!(matches!(
            file.get_msg("recv_single_emoji")?.kind,
            MessageKind::Meable
        ));
        assert!(matches!(
            file.get_msg("send_double_emoji")?.kind,
            MessageKind::Meable
        ));
        assert!(matches!(
            file.get_msg("recv_double_emoji")?.kind,
            MessageKind::Meable
        ));
        assert!(matches!(
            file.get_msg("send_normal")?.kind,
            MessageKind::Normal
        ));
        assert!(matches!(
            file.get_msg("recv_normal")?.kind,
            MessageKind::Normal
        ));

        Ok(())
    }

    #[test]
    fn test_parse_attributed_body() -> Result<()> {
        let file = TestFile::new("tests/messages.json")?;

        let row = file.get_row("send_single_emoji")?;

        let body = AttributedBody::parse(row.attributed_body.as_ref().unwrap())?;

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

    #[test]
    fn test_multiple_emoji() -> Result<()> {
        let file = TestFile::new("tests/emojis.json")?;

        assert!(is_emoji_only(&file.get_msg("one")?.text));
        assert!(is_emoji_only(&file.get_msg("two")?.text));

        // TODO: we need more info for these...
        // should be that 1-3 emojis are meable, but 4+ are not since they don't get blown up
        // this is possibly exposed in message.message_summary_info?
        // three has spaces, but should still be meable
        // four has spaces so it's identified as not meable, but if we fix the regex to allow
        // spaces then it would be meable too when it shouldn't be
        assert!(is_emoji_only(&file.get_msg("three")?.text));
        assert!(!is_emoji_only(&file.get_msg("four")?.text));

        Ok(())
    }

    struct TestFile {
        rows: HashMap<String, MessageRow>,
        msgs: HashMap<String, Message>,
    }

    impl TestFile {
        fn new(path: &str) -> Result<Self> {
            let file = std::fs::File::open(path)?;
            let stream: Vec<serde_json::Value> = serde_json::from_reader(file)?;

            let rows: HashMap<String, MessageRow> = stream
                .into_iter()
                .map(|row| MessageRow {
                    guid: row
                        .get("guid")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_default(),
                    date: row.get("date").and_then(|v| v.as_i64()),
                    id: row
                        .get("id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    text: row
                        .get("text")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    attributed_body: row
                        .get("attributedBody")
                        .and_then(|v| v.as_str())
                        .map(|s| s.strip_prefix("0x").unwrap_or(s))
                        .map(hex::decode)
                        .transpose()
                        .unwrap_or_else(|_| {
                            panic!(
                                "failed to decode attributed body for guid {}",
                                row.get("guid")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("<unknown>")
                            )
                        }),
                    has_attachment: row.get("has_attachment").and_then(|v| v.as_i64()),
                    balloon_bundle_id: row
                        .get("balloon_bundle_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    thread_originator_guid: row
                        .get("thread_originator_guid")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                })
                .map(|row| (row.guid.clone(), row))
                .collect();

            let msgs = rows
                .iter()
                .map(|(guid, row)| {
                    let msg: Message = row.try_into().unwrap();
                    (guid.clone(), msg)
                })
                .collect();

            Ok(Self { rows, msgs })
        }

        fn get_row(&self, name: &str) -> Result<&MessageRow> {
            self.rows
                .get(name)
                .ok_or_else(|| eyre!("message with guid {} not found", name))
        }

        fn get_msg(&self, name: &str) -> Result<&Message> {
            let msg = self
                .msgs
                .get(name)
                .ok_or_else(|| eyre!("message with guid {} not found", name))?;

            println!("{:?}", msg);

            Ok(msg)
        }
    }
}
