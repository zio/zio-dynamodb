package zio.dynamodb.examples.dynamodblocal

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import zio.prelude._
import zio.{ Console, ULayer, ZIO, ZIOAppDefault, ZLayer }

import java.net.URI
import java.time.Instant
import java.util
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * A typical example of scala app using the AWS java SDK. Note how much boiler plate code there is.
 * see [[StudentZioDynamoDbExample]] for the equivalent app using `zio-dynamodb` - note the drastic reduction in boiler
 * plate code!
 */
object StudentJavaSdkExample extends ZIOAppDefault {

  sealed trait Payment
  object Payment {
    case object DebitCard  extends Payment
    case object CreditCard extends Payment
    case object PayPal     extends Payment
  }
  final case class Student(email: String, subject: String, enrollmentDate: Option[Instant], payment: Payment)

  import Payment._

  object DdbHelper {
    import scala.language.implicitConversions

    val ddbLayer: ULayer[DynamoDbAsyncClient] = {
      val effect = ZIO.acquireRelease(for {
        _       <- ZIO.unit
        region   = Region.US_EAST_1
        endpoint = URI.create("http://localhost:8000")
        client   = DynamoDbAsyncClient.builder().endpointOverride(endpoint).region(region).build()
      } yield client)(client => ZIO.attempt(client.close()).ignore)
      ZLayer.fromZIO(ZIO.scoped(effect))
    }

    def createTableRequest: CreateTableRequest = {
      implicit def attrDef(t: (String, ScalarAttributeType)): AttributeDefinition =
        AttributeDefinition.builder.attributeName(t._1).attributeType(t._2).build
      implicit def keySchemaElmt(t: (String, KeyType)): KeySchemaElement          =
        KeySchemaElement.builder.attributeName(t._1).keyType(t._2).build
      implicit def seqAsJava[T](seq: Seq[T]): util.List[T]                        = seq.asJava

      val tableName                                      = "student"
      val attributeDefinitions: Seq[AttributeDefinition] = Seq(
        "email"   -> ScalarAttributeType.S,
        "subject" -> ScalarAttributeType.S
      )

      val ks: Seq[KeySchemaElement] = Seq(
        "email"   -> KeyType.HASH,
        "subject" -> KeyType.RANGE
      )
      val provisionedThroughput     = ProvisionedThroughput.builder
        .readCapacityUnits(5L)
        .writeCapacityUnits(5L)
        .build
      val request                   = CreateTableRequest.builder
        .tableName(tableName)
        .attributeDefinitions(attributeDefinitions)
        .keySchema(ks)
        .provisionedThroughput(provisionedThroughput)
        .build
      request
    }

  }

  def parseInstant(s: String): Either[String, Instant] = Try(Instant.parse(s)).toEither.left.map(_.getMessage)

  def getString(map: Map[String, AttributeValue], name: String): Either[String, String] =
    map.get(name).toRight(s"mandatory field $name not found").map(_.s)

  def getStringOpt(
    map: Map[String, AttributeValue],
    name: String
  ): Either[Nothing, Option[String]] = Right(map.get(name).map(_.s))

  def updatePaymentTypeItemRequest(student: Student): UpdateItemRequest = {
    val values: Map[String, AttributeValue] =
      Map(":paymentType" -> AttributeValue.builder.s(student.payment.toString).build)
    UpdateItemRequest.builder
      .tableName("student")
      .key(
        Map(
          "email"   -> AttributeValue.builder.s(student.email).build,
          "subject" -> AttributeValue.builder.s(student.subject).build
        ).asJava
      )
      .updateExpression("set payment = :paymentType")
      .expressionAttributeValues(values.asJava)
      .build
  }

  def batchGetItemRequest(studentPks: Seq[(String, String)]): BatchGetItemRequest = {
    val keysAndAttributes = KeysAndAttributes.builder
      .keys(
        studentPks.map {
          case (email, subject) =>
            Map(
              "email"   -> AttributeValue.builder().s(email).build(),
              "subject" -> AttributeValue.builder().s(subject).build()
            ).asJava
        }.asJava
      )
      .build()

    BatchGetItemRequest.builder
      .requestItems(Map("student" -> keysAndAttributes).asJava)
      .build()
  }

  def batchWriteItemRequest(students: Seq[Student]): BatchWriteItemRequest = {
    val putRequests = students.map { student =>
      val request = PutRequest
        .builder()
        .item(toAttributeValueMap(student).asJava)
        .build()
      WriteRequest.builder().putRequest(request).build()
    }

    BatchWriteItemRequest
      .builder()
      .requestItems(Map("student" -> putRequests.asJava).asJava)
      .build()
  }

  def toAttributeValueMap(student: Student): Map[String, AttributeValue] = {
    val mandatoryFields                                     = Map(
      "email"   -> AttributeValue.builder.s(student.email).build,
      "subject" -> AttributeValue.builder.s(student.subject).build,
      "payment" -> AttributeValue.builder.s {
        student.payment match {
          case DebitCard  => "DebitCard"
          case CreditCard => "CreditCard"
          case PayPal     => "PayPal"
        }
      }.build
    )
    val nonEmptyOptionalFields: Map[String, AttributeValue] = Map(
      "enrollmentDate" -> student.enrollmentDate.map(instant => AttributeValue.builder.s(instant.toString).build)
    ).filter(_._2.nonEmpty).view.map { case (k, v) => (k, v.get) }.toMap
    mandatoryFields ++ nonEmptyOptionalFields
  }

  def attributeValueMapToStudent(item: Map[String, AttributeValue]): Either[String, Student] =
    for {
      email                 <- getString(item, "email")
      subject               <- getString(item, "subject")
      maybeEnrollmentDateAV <- getStringOpt(item, "enrollmentDate")
      maybeEnrollmentDate   <-
        maybeEnrollmentDateAV.fold[Either[String, Option[Instant]]](Right(None))(s => parseInstant(s).map(i => Some(i)))
      payment               <- getString(item, "payment")
      paymentType            = payment match {
                                 case "DebitCard"  => DebitCard
                                 case "CreditCard" => CreditCard
                                 case "PayPal"     => PayPal
                               }
    } yield Student(email, subject, maybeEnrollmentDate, paymentType)

  def batchWriteAndRetryUnprocessed(
    batchRequest: BatchWriteItemRequest
  ): ZIO[DynamoDbAsyncClient, Throwable, BatchWriteItemResponse] = {
    val result = for {
      client   <- ZIO.service[DynamoDbAsyncClient]
      response <- ZIO.fromCompletionStage(client.batchWriteItem(batchRequest))
    } yield response

    result.flatMap {
      case response if response.unprocessedItems().isEmpty => ZIO.succeed(response)
      case response                                        =>
        // very simple recursive retry of failed requests
        // in Production we would have exponential back offs and a timeout
        batchWriteAndRetryUnprocessed(batchRequest =
          BatchWriteItemRequest
            .builder()
            .requestItems(response.unprocessedItems())
            .build()
        )
    }
  }

  def batchGetItemAndRetryUnprocessed(
    batchRequest: BatchGetItemRequest
  ): ZIO[DynamoDbAsyncClient, Throwable, BatchGetItemResponse] = {
    val result = for {
      client   <- ZIO.service[DynamoDbAsyncClient]
      response <- ZIO.fromCompletionStage(client.batchGetItem(batchRequest))
    } yield response

    result.flatMap {
      case response if response.unprocessedKeys.isEmpty => ZIO.succeed(response)
      case response                                     =>
        // very simple recursive retry of failed requests
        // in Production we would have exponential back offs and a timeout
        batchGetItemAndRetryUnprocessed(batchRequest =
          BatchGetItemRequest.builder
            .requestItems(response.unprocessedKeys)
            .build
        )
    }
  }

  val program = for {
    client               <- ZIO.service[DynamoDbAsyncClient]
    enrollmentDate       <- ZIO.fromEither(parseInstant("2021-03-20T01:39:33Z"))
    avi                   = Student("avi@gmail.com", "maths", Some(enrollmentDate), Payment.DebitCard)
    adam                  = Student("adam@gmail.com", "english", Some(enrollmentDate), Payment.CreditCard)
    students              = List(avi, adam)
    _                    <- ZIO.fromCompletionStage(client.createTable(DdbHelper.createTableRequest))
    batchPutRequest       = batchWriteItemRequest(students)
    _                    <- batchWriteAndRetryUnprocessed(batchPutRequest)
    _                    <- ZIO.fromCompletionStage(
                              client.updateItem(updatePaymentTypeItemRequest(avi.copy(payment = Payment.CreditCard)))
                            )
    batchGetItemResponse <- batchGetItemAndRetryUnprocessed(
                              batchGetItemRequest(students.map(st => (st.email, st.subject)))
                            )
    listOfErrorOrStudent  =
      batchGetItemResponse.responses.asScala.get("student").fold[List[Either[String, Student]]](List.empty) {
        javaList =>
          val listOfErrorOrStudent: List[Either[String, Student]] =
            javaList.asScala.map(m => attributeValueMapToStudent(m.asScala.toMap)).toList
          listOfErrorOrStudent
      }
    errorOrStudents       = listOfErrorOrStudent.flip
    _                    <- Console.printLine(s"result=$errorOrStudents")
  } yield errorOrStudents

  override def run = program.provide(DdbHelper.ddbLayer).exitCode
}
