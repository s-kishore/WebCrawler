from elasticsearch import Elasticsearch

es = Elasticsearch()

es.indices.create(index = "assign4-index", body = { "settings": { "analysis" : { "analyzer": { "my_english": { "type": "english", "stopwords_path": "D:\IR\AP89_DATA\AP_DATA\stoplist.txt" }}}}})
es.indices.put_mapping(index = "assign4-index", doc_type = "assign4", body = { "assign4": { "properties": { "text": { "term_vector": "with_positions_offsets_payloads", "type": "string", "analyzer": "my_english" }}}})

