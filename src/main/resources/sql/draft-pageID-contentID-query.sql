select p.id as page_id, c.id as content_id
from page_draft p
inner join content_draft c ON p.CORE_CONTENT_ID = c.id
inner join resource_content_draft rcl on rcl.id = c.id
where rcl.CONTENT_MICROAPP is not null
and rcl.MICROAPP_CONTENT_DEFINITION is not null
and rcl.content_microapp = (select id from microapp where name='liveblogging')
