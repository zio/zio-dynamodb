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
          ent              = Entitlement("id1", "Entitlement1", "2021-03-20T01:39:33.0")
          _               <- ZIO.fromCompletionStage(client.createTable(DdbHelper.createTableRequest))
          putItemRequest   = PutItemRequest.builder
                               .tableName("Entitlement")
                               .item(
                                 Map(
                                   "id"          -> AttributeValue.builder.s(ent.id).build,
                                   "entitlement" -> AttributeValue.builder.s(ent.entitlement).build,
                                   "orderDate"   -> AttributeValue.builder.s(ent.orderDate).build
                                 ).asJava
                               )
                               .build
          _               <- ZIO.fromCompletionStage(client.putItem(putItemRequest))
          getItemRequest   = GetItemRequest.builder
                               .tableName("Entitlement")
                               .key(
                                 Map(
                                   "id"          -> AttributeValue.builder.s(ent.id).build,
                                   "entitlement" -> AttributeValue.builder.s(ent.entitlement).build
                                 ).asJava
                               )
                               .build()
          getItemResponse <- ZIO.fromCompletionStage(client.getItem(getItemRequest))
          item             = getItemResponse.item.asScala
          foundEnt         = for {
                               id          <- item.get("id")
                               entitlement <- item.get("entitlement")
                               orderDate   <- item.get("orderDate")
                             } yield Entitlement(id.s, entitlement.s, orderDate.s)
        } yield assertTrue(foundEnt == Some(ent))
      }.provideCustomLayer(LocalDdbServer.inMemoryLayer ++ DdbHelper.ddbLayer)
    )

}
