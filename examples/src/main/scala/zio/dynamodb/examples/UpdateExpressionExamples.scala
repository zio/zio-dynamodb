package zio.dynamodb.examples

import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.UpdateExpression.Action.{ AddAction, DeleteAction, RemoveAction, SetAction }
import zio.dynamodb._

object UpdateExpressionExamples extends App {
  val set1: SetAction      = $("one[2]").setValue(1)
  val set2: SetAction      = $("one[2]").set($("two"))
  val set3: SetAction      = $("one[2]").setIfNotExists($("two"), "v2")
  val set4: SetAction      = $("one[2]").appendList(List("1"))
  val set5: SetAction      = $("one[2]").prependList(List("1"))
  val add: AddAction       = $("one[2]").add("V2")
  val remove: RemoveAction = $("one[2]").remove
  val delete: DeleteAction = $("one[2]").deleteFromSet("v2")
  val pe8: SetAction       = $("one[2]").setValue(Item("x" -> "x"))

  val ops: UpdateExpression =
    UpdateExpression(
      $("one[2]").setValue(1) +
        $("one[2]").set($("two")) +
        $("one[2]").setIfNotExists($("two"), "v2") +
        $("one[2]").appendList(List("x1", "x2")) +
        $("one[2]").prependList(List("x", "x2")) +
        $("one[2]").add(1) +
        $("one[2]").remove +
        $("one[2]").deleteFromSet(1)
    )

  $("one[2]").setValue("v2")
  $("one[2]").setValue(Set("s"))
  $("one[2]").setValue(List("42".toByte))
  $("one[2]").setValue(List(List("41".toByte)))
  $("one[2]").setValue(1)
  $("one[2]").setValue(Set(1))
  $("one[2]").setValue(List("x"))

  DynamoDBQuery.updateItem("tableName1", PrimaryKey("id" -> 1))($("foo.bar").setValue(2))

}
