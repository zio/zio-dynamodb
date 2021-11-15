package zio.dynamodb.examples.javasdk

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import zio.dynamodb.examples.LocalDdbServer
import zio.dynamodb.examples.javasdk.Payment.{ CreditCard, DebitCard, PayPal }
import zio.test.Assertion.{ equalTo, isRight }
import zio.test._
import zio.{ Has, ZIO }

import java.time.Instant
import scala.jdk.CollectionConverters._
import scala.util.Try

object JavaSdkExample2Spec extends DefaultRunnableSpec {
  override def spec: ZSpec[Environment, Failure] =
    suite("JavaSdkExample suite")(
      testM("BatchWriteItem and BatchGetItem") {
        def parseInstant(s: String): Either[String, Instant] = Try(Instant.parse(s)).toEither.left.map(_.getMessage)

        def getString(map: Map[String, AttributeValue], name: String): Either[String, String] =
          map.get(name).toRight(s"mandatory field $name not found").map(_.s)

        def getStringOpt(
          map: Map[String, AttributeValue],
          name: String
        ): Either[Nothing, Option[String]] = Right(map.get(name).map(_.s))

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
              .item(studentToAttributeValueMap(student).asJava)
              .build()
            WriteRequest.builder().putRequest(request).build()
          }

          BatchWriteItemRequest
            .builder()
            .requestItems(Map("student" -> putRequests.asJava).asJava)
            .build()
        }

        def studentToAttributeValueMap(student: Student): Map[String, AttributeValue] = {
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

        def attributeValueMapToStudent(item: Map[String, AttributeValue]): Either[String, Student] =
          for {
            email                 <- getString(item, "email")
            subject               <- getString(item, "subject")
            maybeEnrollmentDateAV <- getStringOpt(item, "enrollmentDate")
            maybeEnrollmentDate   <- maybeEnrollmentDateAV.fold[Either[String, Option[Instant]]](Right(None))(s =>
                                       parseInstant(s).map(i => Some(i))
                                     )
            payment               <- getString(item, "payment")
            paymentType            = payment match {
                                       case "DebitCard"  => DebitCard
                                       case "CreditCard" => CreditCard
                                       case "PayPal"     => PayPal
                                     }
          } yield Student(email, subject, maybeEnrollmentDate, paymentType)

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

        def processBatchGetItem(
          batchRequest: BatchGetItemRequest
        ): ZIO[Has[DynamoDbAsyncClient], Throwable, BatchGetItemResponse] = {
          val result = for {
            client   <- ZIO.service[DynamoDbAsyncClient]
            response <- ZIO.fromCompletionStage(client.batchGetItem(batchRequest))
          } yield response

          result.flatMap {
            case response if response.unprocessedKeys.isEmpty => ZIO.succeed(response)
            case response                                     =>
              processBatchGetItem(batchRequest =
                BatchGetItemRequest.builder
                  .requestItems(response.unprocessedKeys)
                  .build
              )
          }
        }

        for {
          client               <- ZIO.service[DynamoDbAsyncClient]
          enrollmentDate       <- ZIO.fromEither(parseInstant("2021-03-20T01:39:33Z"))
          expectedStudent1      = Student("avi@gmail.com", "maths", Some(enrollmentDate), Payment.DebitCard)
          expectedStudent2      = Student("adam@gmail.com", "english", Some(enrollmentDate), Payment.CreditCard)
          students              = List(expectedStudent1, expectedStudent2)
          _                    <- ZIO.fromCompletionStage(client.createTable(DdbHelper.createTableRequest))
          batchPutRequest       = batchWriteItemRequest(students)
          _                    <- processBatchWrite(batchPutRequest)
          batchGetItemResponse <- processBatchGetItem(batchGetItemRequest(students.map(st => (st.email, st.subject))))
          listOfErrorOrStudent  =
            batchGetItemResponse.responses.asScala.get("student").fold[List[Either[String, Student]]](List.empty) {
              javaList =>
                val listOfErrorOrStudent: List[Either[String, Student]] =
                  javaList.asScala.map(m => attributeValueMapToStudent(m.asScala.toMap)).toList
                listOfErrorOrStudent
            }
          errorOrStudents       = zio.dynamodb.foreach(listOfErrorOrStudent)(identity)
        } yield assert(errorOrStudents)(isRight(equalTo(students)))
      }.provideCustomLayer(LocalDdbServer.inMemoryLayer ++ DdbHelper.ddbLayer)
    )

}
