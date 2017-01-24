from pprint import pprint

from bson import ObjectId
from pymongo import MongoClient

db_name = 'app'

db = MongoClient()[db_name]

class BaseDocument(object):
    collection_name = None

    @classmethod
    def get_with_id(cls, id):
        return db[cls.collection_name].find_one({'_id': ObjectId(id)})

    @classmethod
    def save(cls, doc):
        if doc.get('_id'):
            doc_count = db[cls.collection_name].find({'_id': doc['_id']}).count()
            if doc_count == 1:
                db[cls.collection_name].update({'_id': doc['_id']}, doc)
            elif doc_count == 0:
                try:
                    db[cls.collection_name].insert(doc)
                except:
                    print "can't save document"
                    pprint(doc)
            elif doc_count > 1:
                pass
        else:
            db[cls.collection_name].insert(doc)

class Team(BaseDocument):
    collection_name = 'projects'

class Source(BaseDocument):
    collection_name = 'channels'

class Tag(BaseDocument):
    collection_name = 'tags'

class TagRule(BaseDocument):
    collection_name = 'tag_rules'

class ObjectDefinition(BaseDocument):
    collection_name = 'object_definitions'

class Task(BaseDocument):
    collection_name = 'tasks'

class AccessToken(BaseDocument):
    collection_name = 'access_tokens'

class MetricTemplate(BaseDocument):
    collection_name = 'metric_templates'
