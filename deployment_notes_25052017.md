## Deployment steps for first release with ES5

## 1) Create the new index with version number

POST e.g. http://upp-concepts-dynpub-eu.in.ft.com/concepts-0.0.1

with mappings.json in this project.

You can check mappings with this endpoint 
GET http://upp-concepts-dynpub-eu.in.ft.com/concepts-0.0.1/_mappings

## 2) Copy all the data from the original concept index into the new concepts-0.0.1 index

POST http://upp-concepts-dynpub-eu.in.ft.com/_reindex

`{
  "source": {
    "index": "concept"
  },
  "dest": {
    "index": "concepts-0.0.1"
  }
}`

This is asynchronous and somehow you can tell its status by the _tasks endpoint. I am usually check that the GET for this collection is the expected total value.
e.g.

GET http://upp-concepts-dynpub-eu.in.ft.com/concepts-0.0.1/_search
hits.total = 7666522



## 3) Create alias CONCEPTS that points to the versioned index

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
} `

## 4) Check Alias works with a typeahead example.

POST upp-concepts-dynpub-eu.in.ft.com/concepts/_search

`{
  "suggest" : {
	    "mysuggestion" :  {
	    "text" : "Lucy K",
	    "completion" : {
	      "field" : "prefLabel.indexCompletion"
	    }
    }
  }
} `
