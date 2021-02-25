package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery.CreateTable
import zio.dynamodb.{ AttributeDefinition, AttributeDefinitions, KeySchema, NonEmptySet, TableName }
import zio.dynamodb.AttributeValueType._

object MutationQueryExamples extends App {
  val createTable = CreateTable(
    tableName = TableName("someTable"),
    keySchema = KeySchema("hashKey", "sortKey"),
    attributeDefinitions = AttributeDefinitions(AttributeDefinition("attr1", String))
  )

  val x = NonEmptySet("1")
  val y = x + "1"
  println(y.toSet)
}
