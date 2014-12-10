select p.id as page_id, c.id as content_id, lmd.modified_on as last_modified
from page_live p
inner join content_live c ON p.CORE_CONTENT_ID = c.id
inner join resource_content_live rcl on rcl.id = c.id
inner join LAST_MODIFIED_DETAIL lmd on lmd.id = p.LAST_MODIFIED_ID
where rcl.CONTENT_MICROAPP is not null
and rcl.MICROAPP_CONTENT_DEFINITION is not null
and rcl.content_microapp = (select id from microapp where name='liveblogging')
