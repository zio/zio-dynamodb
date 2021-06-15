package zio.dynamodb.examples

import zio.dynamodb.AttributeDefinition._
import zio.dynamodb.DynamoDBQuery.createTable
import zio.dynamodb._

object TableExamples extends App {
  val createTableExample =
    createTable(
      tableName = "someTable",
      KeySchema("hashKey", "sortKey"),
      BillingMode.provisioned(readCapacityUnit = 10, writeCapacityUnit = 10)
    )(
      attrDefnString("attr1"),
      attrDefnNumber("attr2")
    )
      .gsi(
        "indexName",
        KeySchema("key2", "sortKey2"),
        ProjectionType.Include("nonKeyField1", "nonKeyField2"),
        readCapacityUnit = 10,
        writeCapacityUnit = 10
      )
      .gsi(
        "indexName2",
        KeySchema("key2", "sortKey2"),
        ProjectionType.Include("nonKeyField1", "nonKeyField2"),
        readCapacityUnit = 10,
        writeCapacityUnit = 10
      )
      .lsi("indexName3", KeySchema("hashKey", "sortKey"))
      .lsi("indexName4", KeySchema("hashKey"))

}
