package zio.dynamodb.examples.javasdk

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, GetItemRequest, PutItemRequest }
import zio.ZIO
import zio.dynamodb.examples.LocalDdbServer
import zio.test.{ assertTrue, DefaultRunnableSpec, ZSpec }

import java.time.Instant
import scala.jdk.CollectionConverters._
import scala.util.Try

object JavaSdkExample1Spec extends DefaultRunnableSpec {
  override def spec: ZSpec[Environment, Failure] =
    suite("JavaSdkExample suite")(
      testM("sdk example") {
        def parseInstant(s: String): Either[String, Instant] = Try(Instant.parse(s)).toEither.left.map(_.getMessage)

        def getString(map: scala.collection.mutable.Map[String, AttributeValue], name: String): Either[String, String] =
          map.get(name).toRight(s"mandatory field $name not found").map(_.s)

        def getStringOpt(
          map: scala.collection.mutable.Map[String, AttributeValue],
          name: String
        ): Either[Nothing, Option[String]] = Right(map.get(name).map(_.s))

        def putItemRequestForStudent(student: Student) = {
          val mandatoryFields                                     = Map(
            "email"   -> AttributeValue.builder.s(student.email).build,
            "subject" -> AttributeValue.builder.s(student.subject).build
          )
          val nonEmptyOptionalFields: Map[String, AttributeValue] = Map(
            "enrollmentDate" -> student.enrollmentDate.map(instant => AttributeValue.builder.s(instant.toString).build)
          ).filter(_._2.nonEmpty).view.mapValues(_.get).toMap
          PutItemRequest.builder
            .tableName("student")
            .item((mandatoryFields ++ nonEmptyOptionalFields).asJava)
            .build
        }

        for {
          client          <- ZIO.service[DynamoDbAsyncClient]
          enrollmentDate  <- ZIO.fromEither(parseInstant("2021-03-20T01:39:33Z"))
          expectedStudent  = Student("avi@gmail.com", "maths", Some(enrollmentDate))
          _               <- ZIO.fromCompletionStage(client.createTable(DdbHelper.createTableRequest))
          putItemRequest   = putItemRequestForStudent(expectedStudent)
          _               <- ZIO.fromCompletionStage(client.putItem(putItemRequest))
          getItemRequest   = GetItemRequest.builder
                               .tableName("student")
                               .key(
                                 Map(
                                   "email"   -> AttributeValue.builder.s(expectedStudent.email).build,
                                   "subject" -> AttributeValue.builder.s(expectedStudent.subject).build
                                 ).asJava
                               )
                               .build()
          getItemResponse <- ZIO.fromCompletionStage(client.getItem(getItemRequest))
          item             = getItemResponse.item.asScala
          foundStudent     = for {
                               email                 <- getString(item, "email")
                               subject               <- getString(item, "subject")
                               maybeEnrollmentDateAV <- getStringOpt(item, "enrollmentDate")
                               maybeEnrollmentDate   <-
                                 maybeEnrollmentDateAV.fold[Either[String, Option[Instant]]](Right(None))(s =>
                                   parseInstant(s).map(i => Some(i))
                                 )
                             } yield Student(email, subject, maybeEnrollmentDate)
        } yield assertTrue(foundStudent == Right(expectedStudent))
      }.provideCustomLayer(LocalDdbServer.inMemoryLayer ++ DdbHelper.ddbLayer)
    )

}
