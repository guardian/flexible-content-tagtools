select t.id AS tag_id, t.internal_name AS tag_internal_name, 
t.external_name AS tag_external_name, t.WORDS_FOR_URL as slug, sctn.id as section_id, sctn.name as section_name,
sctn.WORDS_FOR_URL as section_slug, sctn.CMS_PATH_PREFIX as path_prefix,
p.ID as publication_tag, c.id as contributor_tag,
nb.id as newspaper_book_tag, nbs.id as newspaper_book_section_tag, 
k.id as keyword_tag, tn.id as tone_tag, b.id as blog_tag, 
s.id as series_tag, ct.id as content_type_tag
from tag t
left join SECTION sctn on sctn.id = t.SECTION_ID
left join PUBLICATION p on p.ID = t.ID
left join CONTRIBUTOR c on c.id = t.ID
left join NEWSPAPER_BOOK nb on nb.id = t.ID
left join NEWSPAPER_BOOK_SECTION nbs on nbs.id = t.ID
left join KEYWORD k on k.id = t.ID
left join TONE tn on tn.id = t.ID
left join BLOG b on b.id = t.ID
left join SERIES s on s.id = t.ID
left join CONTENT_TYPE ct on ct.id = t.ID