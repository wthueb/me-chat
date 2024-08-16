select
    message.guid,
    message.date,
    id,
    message.text,
    message.attributedBody,
    message.cache_has_attachments as has_attachment,
    message.balloon_bundle_id
from
    message
inner join chat_message_join on
    message.ROWID = chat_message_join.message_id
inner join chat on
    chat_message_join.chat_id = chat.ROWID
left join handle on
    message.handle_id = handle.ROWID
where
    chat.group_id = 'C1C65CF7-828E-41EF-91A8-179E80849987'
order by
    date asc
