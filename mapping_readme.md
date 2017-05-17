# How to set-up elasticsearch

Before loading the data, please ensure that the index with its related mapping types are already created.
If you do this for an Amazon Elasticsearch domain, consider using Postman with AWS credentials for each of the calls below. The service name is "es". 

## Create new index for elasticsearch

To verify if the index `concept` exists:
`curl -i -XGET elascticsearch-domain/concept`  

You should see that the cluster has 5 shards created, with 1 replica for each, and this [mapping.json](https://github.com/Financial-Times/concept-rw-elasticsearch/blob/master/mapping.json) attached.

If the concept index is not yet created, create it with attaching the mapping.json settings:

`curl -i -XPUT elasticsearch-domain/concept -d {mapping json}`

This will create 5 shards and 1 replica for each (as a default).

If you want to change the mapping type, you will need to create a new index with the new mapping type, and reindex the data from the old index into the new one. See details [here](https://www.elastic.co/guide/en/elasticsearch/reference/2.3/docs-reindex.html). 

# Basic commands for indexing and schema creation

## Ensure index

To check your indexes, you can do:
`curl -i -XGET elascticsearch-domain/_cat/indices?v`

To add an index, without any mapping:
`curl -i -XPUT elascticsearch-domain/{indexname}`

To delete an index (and all the data inside):
`curl -i -XDELETE elascticsearch-domain/{indexname}`


## Ensure mapping

According to your search expectations, different mappings can be applied over an index and its related fields.

In order to correctly use this writer, the following mapping structure should be applied over your index.
The mappings are attached to the indices, so they can be created at the same time.

### Put mapping
`curl -i -XPUT elasticsearch-domain/concept -d {mapping json}`

For our current version of the db, we used the settings from: [mapping.json](https://github.com/Financial-Times/concept-rw-elasticsearch/blob/master/mapping.json).

### Get mapping
`curl -i XGET elasticsearch-domain/concept/_mapping`

### Delete mapping
It is only possible to remove a mapping if the index is also deleted.