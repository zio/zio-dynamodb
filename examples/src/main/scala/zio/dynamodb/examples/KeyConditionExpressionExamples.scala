package zio.dynamodb.examples

import zio.dynamodb.PartitionKeyExpression._
import zio.dynamodb.SortKeyExpression._
import zio.dynamodb._

object KeyConditionExpressionExamples extends App {

  val exprn1: KeyConditionExpression = PartitionKey("partitionKey1") === "x"

  val exprn2: KeyConditionExpression = PartitionKey("partitionKey1") === "x" && SortKey("sortKey1") > "X"

  val exprn3: KeyConditionExpression = PartitionKey("partitionKey1") === "x" && SortKey("sortKey1") === "X"

}
