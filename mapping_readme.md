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

### Some mapping explanations
Mapping 1
`"indexCompletion": {
  "type": "completion"
},`

This allows basic typeahead searching across everything across the index.
See  (https://www.elastic.co/guide/en/elasticsearch/reference/current/search-suggesters.html)

You can query the concepts endpoint on ES like this

POST http://localhost:9200/concepts/_search 

`{
    "suggest" : {
      "mySuggestions" : {
        "text" : "Lucy K",
        "completion" : {
          "field" : "prefLabel.indexCompletion"
        }
      }
    }
}`

This has been applied on the prefLabel field for example.

Mapping 2
`"completionByContext": {
    "type": "completion",
    "contexts": [{
         "name" : "typeContext",
         "type" : "category",
         "path" : "_type"
    }]
}`

This allows a basic typeahead search based on a context (named typeContext) and the defined values (people & brands) based in the values of the field defined in path (_type).
See (https://www.elastic.co/guide/en/elasticsearch/reference/current/suggester-context.html)

    
`{
     "suggest": {
         "mySuggestions" : {
             "text" : "Lucy K",
             "completion" : {
                 "field" : "prefLabel.completionByContext",
                 "contexts": {
                     "typeContext": [ "people", "brands"]
                 }
             }
         }
     }
 }`
 
 
## Aliases and Reindexing

When applying changes to the mapping the whole index needs to be reindexed.

Create the new index with the new mapping using the ES PUT (as described above)
and then reindex the old index into the new index. This will timeout your request and you can query the progress with the _tasks endpoint (I have been checking the collections size)

POST http://upp-concepts-dynpub-eu.in.ft.com/_reindex

`{
  "source": {
    "index": "concepts"
  },
  "dest": {
    "index": "concepts-0.0.1"
  }
}`

Then update the aliases

POST http://upp-concepts-dynpub-eu.in.ft.com/_aliases

`{
  "actions" : [
    {
      "add" : {
        "index" : "concepts-0.0.1",
        "alias" : "concepts"
      }
    }
  ]
}`

aliases.json indicates the current version





