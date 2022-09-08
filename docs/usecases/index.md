---
id: usecases_index
title: "Use Cases"
---

## Codec Customisation

### Default encoding

#### Sealed trait members that are case classes

```scala
sealed trait TrafficLight
final case class Green(rgb: Int) extends TrafficLight 
final case class Red(rgb: Int) extends TrafficLight
final case class Box(trafficLightColour: TrafficLight)
```

The default encoding for `Box(Green(42))` is:

`Map(trafficLightColour -> Map(String(Green) -> Map(String(rgb) -> Number(42))))`

Here an intermediate map is used to identify the member of `TraficLight` using the member class name ie `Map(String(Green) -> Map(...))`

### Sealed trait members that are case objects

```scala
sealed trait TrafficLight
case object GREEN extends TrafficLight 
case object RED extends TrafficLight
final case class Box(trafficLightColour: TrafficLight)
```

The default encoding for `Box(GREEN)` is:

`Map(trafficLightColour -> Map(String(GREEN) -> Null))`

Here an intermediate map is used to identify the member of `TraficLight` ie `Map(String(GREEN) -> Null)`
Note that the `Null` is used as in this case we do not care about the value.

### Alternate encodings
Encodings can be customised through the use of the following annotations `@discriminator`, `@enumOfCaseObjects` and `@id`.
These annotations are useful when working with a legacy DynamoDB database.

The alternate encodings do not introduce another map for the purposes of identification and this leads to a more compact
encoding that may be more intuitive to work with.

The advantage of the default encoding is that it is more uniform and scalable.

#### Sealed trait members that are case classes

```scala
@discriminator("light_type")
sealed trait TrafficLight
final case class Green(rgb: Int) extends TrafficLight
@id("red_traffic_light")
final case class Red(rgb: Int) extends TrafficLight
final case class Amber(@id("red_green_blue") rgb: Int) extends TrafficLight
final case class Box(trafficLightColour: TrafficLight)
```

encoding for an instance of `Box(Green(42))` would be:

`Map(trafficLightColour -> Map(String(rgb) -> Number(42), String(light_type) -> String(Green)))`

We can specify the field name used to identify the case class through the `@discriminator` annotation. The alternate
encoding removes the intermediate map and inserts a new field with a name specified by discriminator annotation and a
value that identifies the member which defaults to the class name.

This can be further customised using the `@id` annotation - encoding for an instance of `Box(Red(42))` would be:

`Map(trafficLightColour -> Map(String(rgb) -> Number(42), String(light_type) -> String(red_traffic_light)))`

The encoding for case class field names can also be customised via `@id` - encoding for an instance of `Box(Amber(42))` would be:

`Map(trafficLightColour -> Map(String(red_green_blue) -> Number(42), String(light_type) -> String(Amber)))`


#### Sealed trait members that are all case objects

```scala
@enumOfCaseObjects
sealed trait TrafficLight
case object GREEN extends TrafficLight 
@id("red_traffic_light")
case object RED extends TrafficLight
final case class Box(trafficLightColour: TrafficLight)
```

We can get a more compact and intuitive encoding of trait members that are case objects by using the `@enumOfCaseObjects`
annotation which encodes to just a value that is the member name. Encoding for an instance of `Box(GREEN)` would be:

`Map(trafficLightColour -> String(GREEN))`

This can be further customised by using the `@id` annotation again - encoding for `Box(RED)` would be

`Map(trafficLightColour -> String(red_traffic_light))`

## DynamoDB Transactions

Transactions are as simple as calling the `.transact` method on a `DynamoDBQuery`. As long as every component of the query is a valid transaction item and the `DyanmoDBQuery` does not have a mix of get and write transaction items. A list of valid items for both types of queries is listed below.


### Write Transactions
```scala
final case class Bill(studentEmail: String, semesters: Int)

val student = Student("avi@gmail.com", "maths")
val bill = Bill("avi@gmail.com", 1)

val putStudent = put("student", student)
val billedStudent = put("billing", bill)
val deleteFromWaitlist = deleteItem("waitlist", PrimaryKey("email" -> student.email))

val studentEnrollmentTransaction = (putStudent zip billedStudent zip deleteFromWaitlist).transact

for {
  _ <- studentEnrollmentTransaction.execute
} yield ()
```

### ReadTransactions

```scala
final case class EnrolledClass(courseId: String, studentEmail: String)

val avi = Student("avi@gmail.com", "maths")
val maths101 = EnrolledClass("mth-101", avi.email)
val maths102 = EnrolledClass("mth-102", avi.email)

val putAvi = put("student", avi)
val putClasses = put("enrolledClass", maths101) zip put("enrolledClass", maths102)

val enrollAvi = (putAvi zip putClasses).transaction
```

### Transaction Failures

DynamoDBQueries using the `.transaction` method will fail at runtime if there are invalid transaction actions such as creating a table, scanning for items, or querying. The [DynamoDB documentation] has a limited number of actions that can be performed for either a read or a write transaction. There is a `.safeTransaction` method that is also available that will return `Either[Throwable, DynamoDBQuery[A]]`.

There are more examples in our [integration tests](../../dynamodb/src/it/scala/zio/dynamodb/LiveSpec.scala).

### Valid Transact Write Items

* PutItem
* DeleteItem
* BatchWriteItem
* UpdateItem
* ConditionCheck


### Valid Transact Get Item

* GetItem
* BatchGetItem


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
