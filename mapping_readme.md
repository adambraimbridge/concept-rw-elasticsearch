# How to set-up elasticsearch

Before loading the data, please ensure that the concept type and the required mappings are applied over your index.

## Ensure index

To check your indexes, you can use the:
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
`curl -i XPUT elasticsearch-domain/concept/_mapping`

### Delete mapping
It is only possible to remove a mapping if the index is also deleted.