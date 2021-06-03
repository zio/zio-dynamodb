package zio.dynamodb.examples

import zio.dynamodb.AttributeDefinition._
import zio.dynamodb.DynamoDBQuery.createTable
import zio.dynamodb.Projection.All
import zio.dynamodb._

object TableExamples extends App {
  val createTableExample = createTable(
    tableName = "someTable",
    keySchema = KeySchema("hashKey", "sortKey"),
    attributeDefinitions =
      NonEmptySet(stringAttrDefn("attr1"), numberAttrDefn("attr2")), // TODO: this still seems clunky
    billingMode = BillingMode.PayPerRequest,
    globalSecondaryIndexes = Set(
      GlobalSecondaryIndex(
        "indexName",
        keySchema = KeySchema("key2", "sortKey2"),
        projection = Projection.Include("3"),
        provisionedThroughput = Some(ProvisionedThroughput(10, 10))
      )
    ),
    localSecondaryIndexes = Set(LocalSecondaryIndex("indexName2", KeySchema("hashKey", "sortKey"), projection = All))
  )

}
