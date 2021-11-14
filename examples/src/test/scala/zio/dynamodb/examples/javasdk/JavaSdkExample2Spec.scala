package zio.dynamodb.examples.javasdk

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import zio.dynamodb.examples.LocalDdbServer
import zio.dynamodb.examples.javasdk.Payment.{ CreditCard, DebitCard, PayPal }
import zio.test.{ assertTrue, DefaultRunnableSpec, ZSpec }
import zio.{ Has, ZIO }

import java.time.Instant
import scala.jdk.CollectionConverters._
import scala.util.Try

object JavaSdkExample2Spec extends DefaultRunnableSpec {
  override def spec: ZSpec[Environment, Failure] =
    suite("JavaSdkExample suite")(
      testM("BatchWriteItem and GetItem") {
        def parseInstant(s: String): Either[String, Instant] = Try(Instant.parse(s)).toEither.left.map(_.getMessage)

        def getString(map: Map[String, AttributeValue], name: String): Either[String, String] =
          map.get(name).toRight(s"mandatory field $name not found").map(_.s)

        def getStringOpt(
          map: Map[String, AttributeValue],
          name: String
        ): Either[Nothing, Option[String]] = Right(map.get(name).map(_.s))

        def batchWriteItemRequest(students: Seq[Student]): BatchWriteItemRequest = {
          val putRequests = students.map { student =>
            val request = PutRequest
              .builder()
              .item(studentAttributeValueMap(student).asJava)
              .build()
            WriteRequest.builder().putRequest(request).build()
          }

          BatchWriteItemRequest
            .builder()
            .requestItems(Map("student" -> putRequests.asJava).asJava)
            .build()
        }

        def studentAttributeValueMap(student: Student): Map[String, AttributeValue] = {
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
          ).filter(_._2.nonEmpty).view.mapValues(_.get).toMap
          mandatoryFields ++ nonEmptyOptionalFields
        }

        def processBatchWrite(
          batchRequest: BatchWriteItemRequest
        ): ZIO[Has[DynamoDbAsyncClient], Throwable, BatchWriteItemResponse] = {
          val result = for {
            client   <- ZIO.service[DynamoDbAsyncClient]
            response <- ZIO.fromCompletionStage(client.batchWriteItem(batchRequest))
          } yield response

          result.flatMap {
            case response if response.unprocessedItems().isEmpty => ZIO.succeed(response)
            case response                                        =>
              processBatchWrite(batchRequest =
                BatchWriteItemRequest
                  .builder()
                  .requestItems(response.unprocessedItems())
                  .build()
              )
          }
        }

        for {
          client          <- ZIO.service[DynamoDbAsyncClient]
          enrollmentDate  <- ZIO.fromEither(parseInstant("2021-03-20T01:39:33Z"))
          expectedStudent1 = Student("avi@gmail.com", "maths", Some(enrollmentDate), Payment.DebitCard)
          expectedStudent2 = Student("adam@gmail.com", "english", Some(enrollmentDate), Payment.CreditCard)
          _               <- ZIO.fromCompletionStage(client.createTable(DdbHelper.createTableRequest))
          batchPutRequest  = batchWriteItemRequest(List(expectedStudent1, expectedStudent2))
          _               <- processBatchWrite(batchPutRequest)
          getItemRequest   = GetItemRequest.builder
                               .tableName("student")
                               .key(
                                 Map(
                                   "email"   -> AttributeValue.builder.s(expectedStudent1.email).build,
                                   "subject" -> AttributeValue.builder.s(expectedStudent1.subject).build
                                 ).asJava
                               )
                               .build()
          getItemResponse <- ZIO.fromCompletionStage(client.getItem(getItemRequest))
          item             = getItemResponse.item.asScala.toMap
          foundStudent     = for {
                               email                 <- getString(item, "email")
                               subject               <- getString(item, "subject")
                               maybeEnrollmentDateAV <- getStringOpt(item, "enrollmentDate")
                               maybeEnrollmentDate   <-
                                 maybeEnrollmentDateAV.fold[Either[String, Option[Instant]]](Right(None))(s =>
                                   parseInstant(s).map(i => Some(i))
                                 )
                               payment               <- getString(item, "payment")
                               paymentType            = payment match {
                                                          case "DebitCard"  => DebitCard
                                                          case "CreditCard" => CreditCard
                                                          case "PayPal"     => PayPal
                                                        }
                             } yield Student(email, subject, maybeEnrollmentDate, paymentType)
        } yield assertTrue(foundStudent == Right(expectedStudent1))
      }.provideCustomLayer(LocalDdbServer.inMemoryLayer ++ DdbHelper.ddbLayer)
    )

}
