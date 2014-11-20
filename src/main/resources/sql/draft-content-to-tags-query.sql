SELECT content_id, tag_id, ttlc.sort as sort
FROM tag_to_draft_content ttlc
inner join resource_content_draft rcl on rcl.id = ttlc.content_id
where rcl.CONTENT_MICROAPP is not null
and rcl.MICROAPP_CONTENT_DEFINITION is not null
and rcl.content_microapp = (select id from microapp where name='liveblogging')
