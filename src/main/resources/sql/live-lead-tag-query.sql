select content_live_id as content_id, tag_id
from LEAD_CONTENT_LIVE lcl
inner join resource_content_live rcl on rcl.id = lcl.content_live_id
where rcl.CONTENT_MICROAPP is not null
and rcl.MICROAPP_CONTENT_DEFINITION is not null
and rcl.content_microapp = (select id from microapp where name='liveblogging')
