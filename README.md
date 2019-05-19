## ESC

ESC is a lightweight wrapper for [github.com/olivere/elastic](github.com/olivere/elastic) which provides a 
quick setup for some common functions.


It enables quick integrations of bulk uploads to an ES cluster with minimal setup, often used
for prototyping or quick scripts to upload data.  It also provides an easy client
setup for sending to AWS Elasticsearch Service, for use with an assumed role (Lambda, EC2, ECS, etc)


 
#### Basic:
```
esClient := esc.New("https://localhost:9200", "admin", "admin", true)
```


#### AWS 

```
esClient := esc.NewAWS(esHost)
```


#### Batching uploads

The function : esClient.UpsertInterfaceStream provides an easy way to upload any
struct type to es. By putting the list of structs into a channel of interfaces enabled
the elasticsearch client to easily marshal the structs into JSON and upload them
ES.

Make sure to close the channel when finished to end the upload

```
err := esClient.UpsertInterfaceStream(objStream, "my_index")
```





