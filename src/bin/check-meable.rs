use std::collections::HashSet;

use color_eyre::eyre::Result;

use gap_check::{db::get_db_query, message::is_emoji_only};
use regex::Regex;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    dotenvy::dotenv().ok();

    let query = get_db_query().await?;

    let rows = query.as_vec().await?;

    let wordle_regex = Regex::new(r"^Wordle \d+ \d/6").unwrap();

    // these are fluff
    let ignored_attributes = [
        "__kIMMessagePartAttributeName",
        "__kIMBaseWritingDirectionAttributeName",
        "__kIMMentionConfirmedMention",
        "__kIMTextBoldAttributeName",
        "__kIMTextEffectAttributeName",
        "__kIMMoneyAttributeName",
        "__kIMCalendarEventAttributeName",
        "__kIMPhoneNumberAttributeName",
        "__kIMAddressAttributeName",
    ];

    // if a message's attributes are exactly these (plus any ignored attributes), it's not meable
    let not_meable_attributes = [
        HashSet::from(["__kIMDataDetectedAttributeName"]),
        HashSet::from(["__kIMLinkAttributeName"]),
        HashSet::from(["__kIMPhotoSharingAttributeName"]),
        HashSet::from([
            "__kIMOneTimeCodeAttributeName",
            "__kIMDataDetectedAttributeName",
        ]),
    ];

    // if a message's attributes are any superset of any of these, it's meable
    let meable_attributes = [
        HashSet::from(["__kIMFileTransferGUIDAttributeName"]), // images/videos/other files
        HashSet::from(["__kIMLinkAttributeName", "__kIMDataDetectedAttributeName"]), // links
        HashSet::from([
            "__kIMBreadcrumbTextMarkerAttributeName",
            "__kIMBreadcrumbTextOptionFlags",
        ]), // sent via icloud breadcrumb?
    ];

    for i in 0..rows.len() {
        let msg = rows.get(i).unwrap();

        if is_emoji_only(&msg.text) || wordle_regex.is_match(&msg.text) {
            continue;
        }

        if let Some(attributes) = &msg.attributes {
            let attributes = attributes
                .iter()
                .map(|a| a.as_str())
                .collect::<HashSet<_>>();

            let filtered = attributes
                .iter()
                .filter(|a| !ignored_attributes.contains(a))
                .copied()
                .collect::<HashSet<_>>();

            if filtered.is_empty() {
                continue;
            }

            if not_meable_attributes.contains(&filtered) {
                continue;
            }

            if meable_attributes.iter().any(|a| a.is_subset(&attributes)) {
                continue;
            }

            println!("{:?}", msg);

            // let context = (i.saturating_sub(5)..=i + 5)
            //     .filter(|j| *j < rows.len())
            //     .map(|j| rows.get(j).unwrap())
            //     .collect::<Vec<_>>();
            //
            // println!("Context:");
            // for msg in context {
            //     println!("{:?}", msg);
            // }
        }
    }

    Ok(())
}
