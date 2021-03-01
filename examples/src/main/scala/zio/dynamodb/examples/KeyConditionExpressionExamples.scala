package zio.dynamodb.examples

import zio.dynamodb._
import zio.dynamodb.PartitionKeyExpression._
import zio.dynamodb.SortKeyExpression._
import zio.dynamodb.KeyConditionExpression.Operand._

object KeyConditionExpressionExamples extends App {

  val exprn: KeyConditionExpression =
    (PartitionKeyOperand("partitionKey1") == ValueOperand(
      AttributeValue.String("x")
    ))
      .&&(SortKeyOperand("sortKey1").>(ValueOperand(AttributeValue.String("X"))))

  println(exprn)
}
