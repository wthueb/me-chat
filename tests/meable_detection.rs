use gap_check::message::{MeableType, Message, MessageKind};
use imessage_database::tables::{
    handle::Handle,
    messages::Message as DbMessage,
    table::{Cacheable, get_connection},
};
use std::collections::HashMap;
use std::path::Path;

fn setup() -> (rusqlite::Connection, HashMap<i32, String>) {
    let conn = get_connection(Path::new("tests/fixtures.db"))
        .expect("tests/fixtures.db not found. run `cargo run --bin extract-fixtures` first");

    let handles = Handle::cache(&conn).expect("failed to cache handles from fixtures.db");

    (conn, handles)
}

fn load_message(conn: &rusqlite::Connection, guid: &str) -> DbMessage {
    let mut raw = DbMessage::from_guid(guid, conn)
        .unwrap_or_else(|e| panic!("message not found in fixtures.db: {guid} (error: {e})"));

    let _ = raw.generate_text(conn);

    raw
}

fn assert_meable(
    conn: &rusqlite::Connection,
    handles: &HashMap<i32, String>,
    guid: &str,
    expected: Vec<MeableType>,
) {
    let raw = load_message(conn, guid);
    let msg = Message::from_raw(raw, conn, handles).unwrap();

    if let MessageKind::Meable(types) = msg.kind {
        let mut expected_counts: HashMap<MeableType, usize> = HashMap::new();
        for t in expected {
            *expected_counts.entry(t).or_insert(0) += 1;
        }
        let mut actual_counts: HashMap<MeableType, usize> = HashMap::new();
        for t in types {
            *actual_counts.entry(t).or_insert(0) += 1;
        }
        assert_eq!(
            expected_counts, actual_counts,
            "expected {:?}, got {:?} for guid {}",
            expected_counts, actual_counts, guid
        );
    } else {
        panic!(
            "expected message to be meable, but got {:?} for guid {}",
            msg.kind, guid
        );
    }
}

#[allow(dead_code)]
fn assert_kind(
    conn: &rusqlite::Connection,
    handles: &HashMap<i32, String>,
    guid: &str,
    expected: MessageKind,
) {
    let raw = load_message(conn, guid);
    let msg = Message::from_raw(raw, conn, handles).unwrap();
    assert!(
        std::mem::discriminant(&msg.kind) == std::mem::discriminant(&expected),
        "expected {:?}, got {:?} for guid {}",
        expected,
        msg.kind,
        guid
    );
}

#[test]
fn test_multiple_images_is_meable() {
    let (conn, handles) = setup();
    assert_meable(
        &conn,
        &handles,
        "C0950D79-397C-462B-983C-600A51F3F1CA",
        vec![MeableType::Attachment; 6],
    );
}
