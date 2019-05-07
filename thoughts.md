# MongoDB JournalReader

- Assumption: to guarantee atomic writes of multiple events/snapshot, all this events and the snapshot are written into one document

Sample Document:
```json
{
    "_id": ObjectID("..."),
    "streamName": "stream1",
    "entries": [
        {
            "_id": ObjectID("..."),
            "streamVersion": 1,
            "typeVersion": 1,
            "metadata": "json serialized meta data map",
            "document": {
                "json": "e.g. json serialized event content"
            },
            "type": "fully.qualified.Classname"
        }, 
        {
            "_id": ObjectID("..."),
            "streamVersion": 2,
            "typeVersion": 1,
            "metadata": "json serialized meta data map",
            "document": {
                "json": "e.g. json serialized event content"
            },
            "type": "fully.qualified.Classname"
        }
    ],
    "state": {
        "document": { 
            "json": "e.g. json serialized state content"
        },
        "type": "fully.qualified.Classname",
        "dataVersion": "$streamVersionStart",
        "typeVersion": "1",
        "metadata": "json serialized meta data map"
    },
    "hasSnapshot": true,
    "streamVersionStart": 1,
    "streamVersionEnd": 2,
}
```  

## Global ordering

## at-least-once delivery

To ensure eventual consistency of down-stream systems, we need to guarantee that a `JournalReader` does not miss any event.

### Cursor based

- Assumption: each `JournalReader` exits only once, even in a cluster

```json
{
    "_id": ObjectID("..."),
    "name": "stream-reader-name",
    "documentId": ObjectID("of the current document"),
    "entryIndex": 1, 
    "hasMoreElements": true 
}
``` 
 
#### Global Counter
A global counter could be achieved using the standard pattern for sequences in MongoDB: https://www.mongodb.com/blog/post/generating-globally-unique-identifiers-for-use-with-mongodb

Problem  
Having multiple `Journal` writing to a stream (e.g. in a Cluster). This could break the ordering:

consider two Journals `J1` and `J2` and a sequence `S` and a `JournalReader` `JR`

| `J1` | `J2` | `JR` |
|---|---|---|
| retrieves `2` from `S` | | looks for events > `1` |  
|  | retrieves `3` from `S` | looks for events > `1` |
|  | writes event `3` to `Journal` | finds `3` |
| writes event `2` to `Journal` | | looks for events > `3`|
Event `2` will never be read 

#### TimeStamp w/ `$currentTime`

Each document could have a field `"insertedAt": "$currentTime"`. Thus MongoDB would replace the field with a timestamp 
if it is inserted. 

Problem:
As this might work for a single node or a replica set, a sharded cluster might break is if the wall clock on the machines 
is different. 

#### ObjectId
 
Similar to [TimeStamp](#timestamp-w-currenttime) approach. In addition the resolution of the `ObjectId`s 
timestamp just down to seconds, leaves more room for missing a document. 
  

#### Natural Order using `$natural`
Natural order could somehow work for single instances or replica sets, but for shards it is non-deterministic.

So not an option.


### Commit based

#### Within each document



#### In a separate document



#### Batch Token

Generate a UUID and store a batch document before the actual event document. Refer the `batchId` in the event document.

Problem:  
Distinguishing between a batch that isn't just inserted (but will be later on) and a batch whose insert failed and thus it will never appear.       

### Tail based


  
