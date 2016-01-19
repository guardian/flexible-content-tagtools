db.draftContent.find({"taxonomy.publication":{$exists:false}})

db.draftContent.find({$and:[{"taxonomy":{$exists:true}},{"taxonomy.publication":{$exists:false}}]}).count()

db.draftContent.find({"taxonomy":{$exists:false}}).count()