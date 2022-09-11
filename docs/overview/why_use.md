---
id: overview_why_use
title: "Why ZIO-DynamoDB"
---

ZIO-DynamoDB provides one of the best boilerplate free APIs for interacting with DynamoDB. Compared to the Java SDK you will see a massive reduction in code that you need to maintain. Below we have an example spread out across a few different code blocks. Each block is an incremental step towards using DynamoDB with the Java SDK.

```scala
final case class Student(
  email: String,            // partition key 
  subject: String,          // sort key
  enrollmentDate: Option[Instant], 
  payment: Payment
)

sealed trait Payment
object Payment {
  final case object DebitCard  extends Payment
  final case object CreditCard extends Payment
  final case object PayPal     extends Payment
}
```
Our case class for the example. We plan on writing a few students to DynamoDB. Note we have an optional field, `enrollmentDate` as well as a sealed trait `payment`.

In order to write a Student to DynamoDB we need to build a `PutItemRequest` for our case class.

```scala
def putStudentRequest(student: Student): PutItemRequest =
  PutItemRequest.builder
    .tableName("student")
    .item(toAttributeValueMap(student).asJava) //Item: Map[String, AttributeValue]
    .build

def toAttributeValueMap(student: Student): Map[String, AttributeValue] = {
  val mandatoryFields                                     = Map(
    "email"   -> AttributeValue.builder.s(student.email).build,
    "subject" -> AttributeValue.builder.s(student.subject).build,
    "payment" -> AttributeValue.builder.s { // serialize sum type
      student.payment match {
        case DebitCard  => "DebitCard"
        case CreditCard => "CreditCard"
        case PayPal     => "PayPal"
      }
    }.build
  )

  val nonEmptyOptionalFields: Map[String, AttributeValue] = Map(
    "enrollmentDate" -> student.enrollmentDate.map(instant =>
      AttributeValue.builder.s(instant.toString).build)
  ).filter(_._2.nonEmpty).map { case (k, v) => (k, v.get) }

  mandatoryFields ++ nonEmptyOptionalFields
}
```
Here we have written some helper functions to serialize a Student into a `PutItemRequest` for the Java SDK.


Now that we can write a Student to DynamoDB we need to be able to read them back and deserialize them.

```scala
def getStudentRequest(email: String, subject: String): GetItemRequest = {
  GetItemRequest.builder
    .tableName("student")
    .key(
      Map(
        "email" -> AttributeValue.builder.s(email).build,
        "subject" -> AttributeValue.builder.s(subject).build
      ).asJava
    )
    .build()
}

def getString(map: Map[String, AttributeValue],
              name: String): Either[String, String] =
  map.get(name).toRight(s"mandatory field $name not found").map(_.s)

def getStringOpt(map: Map[String, AttributeValue],
                 name: String): Either[Nothing, Option[String]] =
  Right(map.get(name).map(_.s))

def parseInstant(s: String): Either[String, Instant] =
  Try(Instant.parse(s)).toEither.left.map(_.getMessage)

def attributeValueMapToStudent(item: Map[String, AttributeValue])
: Either[String, Student] =
  for {
    email                 <- getString(item, "email")
    subject               <- getString(item, "subject")
    maybeEnrollmentDateAV <- getStringOpt(item, "enrollmentDate")
    maybeEnrollmentDate   <-
      maybeEnrollmentDateAV.fold[Either[String, Option[Instant]]](Right(None))(s =>
        parseInstant(s).map(i => Some(i))
      )
    payment               <- getString(item, "payment")
    paymentType           <- payment match {
      case "DebitCard"  => Right(DebitCard)
      case "CreditCard" => Right(CreditCard)
      case "PayPal"     => Right(PayPal)
      case _            => Left("invalid payment type")
    }
  } yield Student(email, subject, maybeEnrollmentDate, paymentType)
```

In this code block we have setup a number of functions to assist with creating a `GetItemRequest` as well as deserialize the response to a Student. 

Let's put all of these functions together to create a full program.

```scala
val program = 
for {
  client            <- ZIO.service[DynamoDbAsyncClient]
  student            = Student("avi@gmail.com", "maths", Instant.now, 
                       Payment.DebitCard)

  putRequest         = putStudentRequest(student)
  _                 <- ZIO.fromCompletionStage(client.putItem(putRequest))
  getItemRequest     = getStudentRequest(student.email, student.subject)
  getItemResponse   <- ZIO.fromCompletionStage(client.getItem(getItemRequest))
  studentItem        = getItemResponse.item.asScala.toMap
  foundStudent       = deserialise(studentItem)
} yield foundStudent
```

Excluding the case class definition and the full program, that's 65 additional lines of code exclusively for handling writing to and reading from DynamoDB. It doesn't even begin to cover the more complex query types, automatic batching, and automatic parallaisation that ZIO-DynamoDB provides. 

Let's look at the same program using ZIO-DynamoDB instead of using the Java SDK.

```scala
val liveDynamoDBExecutorLayer = ...

object Student {
  implicit lazy val schema: Schema[Student] = DeriveSchema.gen[Student]
}

val errorOrListOfStudent = (for {
  avi          = Student("avi@gmail.com", "maths", ...)
  adam         = Student("adam@gmail.com", "english", ...)

  _            <- (put[Student]("student", avi) zip put[Student]("student", adam)).execute

  listOfErrorOrStudent <- forEach(List(avi, adam)) { st =>
      get[Student]("student",
        PrimaryKey("email" -> st.email, "subject" -> st.subject)
      )}.execute

} yield EitherUtil.collectAll(listOfErrorOrStudent))
  .provideLayer(liveDynamoDBExecutorLayer)
```

ZIO-DynamoDB leverages ZIO-Schema to provide serialization for developers out of the box. The amount of boilerplate code ZIO-DynamoDB removes for your code base means engineers have more time writing business logic and less time maintaining their integration with DynamoDB.


There was also a talk at [Functional Scala 2021](https://www.youtube.com/watch?v=fKUIfb5ufxw&t=7047s) that covers the drastic reduction in code that results from migrating to ZIO-DynamoDB from the Java SDK.


