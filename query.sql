select
    message.guid,
    message.date,
    handle.id,
    message.text,
    message.attributedBody as attributed_body,
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
