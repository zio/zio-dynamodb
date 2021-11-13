package zio.dynamodb.examples.javasdk

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, GetItemRequest, PutItemRequest }
import zio.ZIO
import zio.dynamodb.examples.LocalDdbServer
import zio.test.{ assertTrue, DefaultRunnableSpec, ZSpec }

import scala.jdk.CollectionConverters._

object JavaSdkExampleSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[Environment, Failure] =
    suite("JavaSdkExample suite")(
      testM("sdk example") {

        for {
          client          <- ZIO.service[DynamoDbAsyncClient]
          student          = Student("avi@gmail.com", "maths", "2021-03-20T01:39:33.0")
          _               <- ZIO.fromCompletionStage(client.createTable(DdbHelper.createTableRequest))
          putItemRequest   = PutItemRequest.builder
                               .tableName("student")
                               .item(
                                 Map(
                                   "email"          -> AttributeValue.builder.s(student.email).build,
                                   "subject"        -> AttributeValue.builder.s(student.subject).build,
                                   "enrollmentDate" -> AttributeValue.builder.s(student.enrollmentDate).build
                                 ).asJava
                               )
                               .build
          _               <- ZIO.fromCompletionStage(client.putItem(putItemRequest))
          getItemRequest   = GetItemRequest.builder
                               .tableName("student")
                               .key(
                                 Map(
                                   "email"   -> AttributeValue.builder.s(student.email).build,
                                   "subject" -> AttributeValue.builder.s(student.subject).build
                                 ).asJava
                               )
                               .build()
          getItemResponse <- ZIO.fromCompletionStage(client.getItem(getItemRequest))
          item             = getItemResponse.item.asScala
          foundStudent     = for {
                               email          <- item.get("email")
                               subject        <- item.get("subject")
                               enrollmentDate <- item.get("enrollmentDate")
                             } yield Student(email.s, subject.s, enrollmentDate.s)
        } yield assertTrue(foundStudent == Some(student))
      }.provideCustomLayer(LocalDdbServer.inMemoryLayer ++ DdbHelper.ddbLayer)
    )

}
