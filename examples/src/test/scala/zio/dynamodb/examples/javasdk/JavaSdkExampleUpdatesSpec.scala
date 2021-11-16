package zio.dynamodb.examples.javasdk

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  GetItemRequest,
  PutItemRequest,
  UpdateItemRequest
}
import zio.ZIO
import zio.dynamodb.examples.LocalDdbServer
import zio.dynamodb.examples.javasdk.Payment.{ CreditCard, DebitCard, PayPal }
import zio.test.{ assertTrue, DefaultRunnableSpec, ZSpec }

import java.time.Instant
import scala.jdk.CollectionConverters._
import scala.util.Try

object JavaSdkExampleUpdatesSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[Environment, Failure] =
    suite("JavaSdkExample suite")(
      testM("UpdateItems") {
        def parseInstant(s: String): Either[String, Instant] = Try(Instant.parse(s)).toEither.left.map(_.getMessage)

        def getString(map: Map[String, AttributeValue], name: String): Either[String, String] =
          map.get(name).toRight(s"mandatory field $name not found").map(_.s)

        def getStringOpt(
          map: Map[String, AttributeValue],
          name: String
        ): Either[Nothing, Option[String]] = Right(map.get(name).map(_.s))

        def putItemRequest(student: Student): PutItemRequest =
          PutItemRequest.builder
            .tableName("student")
            .item(toAttributeValueMap(student).asJava)
            .build

        def updateItemRequest(student: Student): UpdateItemRequest = {
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
          ).filter(_._2.nonEmpty).view.mapValues(_.get).toMap
          mandatoryFields ++ nonEmptyOptionalFields
        }

        for {
          client          <- ZIO.service[DynamoDbAsyncClient]
          enrollmentDate  <- ZIO.fromEither(parseInstant("2021-03-20T01:39:33Z"))
          avi              = Student("avi@gmail.com", "maths", Some(enrollmentDate), Payment.DebitCard)
          adam             = Student("adam@gmail.com", "english", Some(enrollmentDate), Payment.CreditCard)
          _               <- ZIO.fromCompletionStage(client.createTable(DdbHelper.createTableRequest))
          request          = putItemRequest(avi)
          _               <- ZIO.fromCompletionStage(client.putItem(request))
          updatedAvi       = avi.copy(payment = Payment.PayPal)
          updatedAdam      = adam.copy(payment = Payment.PayPal)
          _               <- ZIO.fromCompletionStage(client.updateItem(updateItemRequest(updatedAvi))) zipPar ZIO.fromCompletionStage(
                               client.updateItem(updateItemRequest(updatedAdam))
                             )
          getItemRequest   = GetItemRequest.builder
                               .tableName("student")
                               .key(
                                 Map(
                                   "email"   -> AttributeValue.builder.s(avi.email).build,
                                   "subject" -> AttributeValue.builder.s(avi.subject).build
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
        } yield assertTrue(foundStudent == Right(updatedAvi))
      }.provideCustomLayer(LocalDdbServer.inMemoryLayer ++ DdbHelper.ddbLayer)
    )

}
