select old_tag_id from tag_audit where performed_on > to_date('02-OCT-11','DD-MON-YY') and operation = 'delete';