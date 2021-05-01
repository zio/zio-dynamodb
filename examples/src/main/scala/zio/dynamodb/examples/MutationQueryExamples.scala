package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery.CreateTable
import zio.dynamodb.Projection.{ All, Include }
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

}
