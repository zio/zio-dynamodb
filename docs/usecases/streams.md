---
id: usecases_streams
title: "Streams"
---

## Using Streams to Read or Write

There are a pair of APIs to read or write large streams of data to/from DynamoDB. These APIs are provided to avoid exceeding the maximum limit of 25 items in batch get/write requests. Zipping over 25 requests together into a single `BatchGetItem` will result in a runtime error from AWS. In the event that you need to make a large number of requests you should use these streaming methods which will group your requests into chunks of 25 items and batch those.

```scala
final case class Person(id: Int, name: String)
object Person {
  implicit val schema: Schema[Person] = DeriveSchema.gen[Person]
}

private val personStream: UStream[Person] =
  ZStream.fromIterable(1 to 20).map(i => Person(i, s"name$i"))

def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
  (for {
    // write to DB using the stream as the source of the data to write
    // write queries will automatically be batched using BatchWriteItem when calling DynamoDB
    _ <- batchWriteFromStream(personStream) { person =>
      putItem("person", Item("id" -> person.id, "name" -> person.name))
    }.runDrain

    // read from the DB using the stream as the source of the primary key
    // read queries will automatically be batched using BatchGetItem when calling DynamoDB
    _ <- batchReadItemFromStream[Console, Person]("person", personStream)(person => PrimaryKey("id" -> person.id))
      .mapMPar(4)(item => putStrLn(s"item=$item"))
      .runDrain

    // same again but use Schema derived codecs to convert an Item to a Person
    _ <- batchReadFromStream[Console, Person]("person", personStream)(person => PrimaryKey("id" -> person.id))
      .mapMPar(4)(person => putStrLn(s"person=$person"))
      .runDrain
  } yield ()).provideCustomLayer(DynamoDBExecutor.test).exitCode
```
