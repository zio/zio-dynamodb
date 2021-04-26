package zio.dynamodb.examples

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.CreateTable
import zio.dynamodb.Projection.{ All, Include }
import zio.dynamodb.ProjectionExpression.TopLevel
import zio.dynamodb._

object MutationQueryExamples extends App {
  val createTable = CreateTable(
    tableName = TableName("someTable"),
    keySchema = KeySchema("hashKey", "sortKey"),
    attributeDefinitions = NonEmptySet(AttributeDefinition("attr1", AttributeValueType.String)) + AttributeDefinition(
      "attr2",
      AttributeValueType.Number
    ),
    billingMode = BillingMode.PayPerRequest,
    globalSecondaryIndexes = Set(
      GlobalSecondaryIndex(
        IndexName("1"),
        keySchema = KeySchema("key2", "sortKey2"),
        projection = Include("3"),
        provisionedThroughput = Some(ProvisionedThroughput(10, 10))
      )
    ),
    localSecondaryIndexes = Set(LocalSecondaryIndex(IndexName("1"), KeySchema("hashKey", "sortKey"), projection = All))
  )

  /*
  $("one[2]")
  $("foo.bar[9].baz")
   */
  val path1      = TopLevel("one")(2)
  val pe         = path1.set("v2")
  val pe2        = path1.set(Set("s"))
  val pe3        = path1.set(Chunk("s".toByte))
  val pe4        = path1.set(Chunk(Chunk("s".toByte)))
  val pe5        = path1.set(BigDecimal(1.0))
  val pe6        = path1.set(Set(BigDecimal(1.0)))
  val pe7        = path1.set(Chunk("x")) // TODO
  val updateItem = DynamoDBQuery.updateItem(TableName("t1"), PrimaryKey(Map.empty), pe)

  val x = AttributeValue.Map(Map(AttributeValue.String("") -> AttributeValue.String("")))

  /*
  val pe8        = path1.set(ScalaMap("x" -> "x"))
   */

}
