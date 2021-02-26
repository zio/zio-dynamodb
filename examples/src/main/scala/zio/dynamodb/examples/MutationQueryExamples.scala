package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery.CreateTable
import zio.dynamodb.{ AttributeDefinition, AttributeValueType, KeySchema, NonEmptySet, TableName }

object MutationQueryExamples extends App {
  val createTable = CreateTable(
    tableName = TableName("someTable"),
    keySchema = KeySchema("hashKey", "sortKey"),
    attributeDefinitions = NonEmptySet(AttributeDefinition("attr1", AttributeValueType.String)) + AttributeDefinition(
      "attr2",
      AttributeValueType.Number
    )
  )

  val x = NonEmptySet("1") ++ NonEmptySet("2")
  val y = x + "3"
  println(y.toSet)
}
