package zio.dynamodb.examples.javasdk

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import zio.{ Has, ULayer, ZIO, ZManaged }

import java.net.URI
import java.util
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

object DdbHelper {
  val ddbLayer: ULayer[Has[DynamoDbAsyncClient]] = ZManaged
    .make(for {
      _       <- ZIO.unit
      region   = Region.US_EAST_1
      endpoint = URI.create("http://localhost:8000")
      client   = DynamoDbAsyncClient.builder().endpointOverride(endpoint).region(region).build()
    } yield client)(client => ZIO.effect(client.close()).ignore)
    .toLayer

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
