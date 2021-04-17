package zio.dynamodb.examples

import zio.dynamodb.PartitionKeyExpression._
import zio.dynamodb.SortKeyExpression._
import zio.dynamodb._

object KeyConditionExpressionExamples extends App {

  val exprn: KeyConditionExpression =
    PartitionKey("partitionKey1") == AttributeValue.String("x") &&
      SortKey("sortKey1") > AttributeValue.String("X")

  println(exprn)

}
