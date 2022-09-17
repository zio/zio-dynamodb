package zio.dynamodb.examples

import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.UpdateExpression.Action.{ AddAction, DeleteAction, RemoveAction, SetAction }
import zio.dynamodb._

object UpdateExpressionExamples extends App {
  val set1: SetAction[Any, Int]         = $("one[2]").set(1)
  val set2: SetAction[Any, Any]         = $("one[2]").set($("two"))
  val set3: SetAction[Any, String]      = $("one[2]").setIfNotExists($("two"), "v2")
  val set4: SetAction[Any, String]      = $("one[2]").appendList(List("1"))
  val set5: SetAction[Any, String]      = $("one[2]").prependList(List("1"))
  val add: AddAction[String]            = $("one[2]").add("V2")
  val addSet: AddAction[Set[String]]    = $("one[2]").addSet(Set("V2"))
  val remove: RemoveAction[_]           = $("one[2]").remove
  val removeAtIndex: RemoveAction[Any]  = $("one[2]").remove(1)
  val delete: DeleteAction[Set[String]] = $("one[2]").deleteFromSet(Set("v2"))
  val pe8: SetAction[Any, PrimaryKey]   = $("one[2]").set(Item("x" -> "x"))

  val ops =
    UpdateExpression(
      $("one[2]").set(1) +
        $("one[2]").set($("two")) +
        $("one[2]").setIfNotExists("v2") +
        $("one[2]").setIfNotExists($("two"), "v2") +
        $("one[2]").appendList(List("x1", "x2")) +
        $("one[2]").prependList(List("x", "x2")) +
        $("one[2]").add(1) +
        $("one[2]").remove +
        $("one[2]").deleteFromSet(Set(1))
    )

  $("one[2]").set("v2")
  $("one[2]").set(Set("s"))
  $("one[2]").set(List("42".toByte))
  $("one[2]").set(List(List("41".toByte)))
  $("one[2]").set(1)
  $("one[2]").set(Set(1))
  $("one[2]").set(List("x"))

  DynamoDBQuery.updateItem("tableName1", PrimaryKey("id" -> 1))($("foo.bar").set(2))

}
