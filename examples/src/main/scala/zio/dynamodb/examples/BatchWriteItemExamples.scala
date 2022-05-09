package zio.dynamodb.examples

import zio.dynamodb.ConditionExpression.Operand.ToOperand
import zio.dynamodb.DynamoDBQuery.{ deleteItem, putItem }
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb._

object BatchWriteItemExamples extends App {

  implicit val x  = implicitly[ToOperand[String]]
  val batchManual =
    (putItem("table1", Item("field1" -> 1)) where $("a.b") === "1") <*> deleteItem(
      "table2",
      PrimaryKey("primaryKey" -> 1)
    ) where $("c.b") === "2"
  println(batchManual)

  val batchPutFromIterable = DynamoDBQuery
    .forEach(1 to 3) { i =>
      putItem("table1", Item("field1" -> i.toString))
    }
    .where($("foo.bar") > "1")
  println(batchPutFromIterable)

  val batchDeleteFromIterable = DynamoDBQuery.forEach(1 to 3) { i =>
    deleteItem("tableName1", PrimaryKey("pk" -> i.toString)) where $("foo.bar") > "1" && !($("foo.bar") < "5")
  }
  println(batchDeleteFromIterable)

}
