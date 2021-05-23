package zio.dynamodb.examples

import zio.dynamodb.ProjectionExpression.Root
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb.UpdateExpression.Action.{ AddAction, DeleteAction, RemoveAction, SetAction }
import zio.dynamodb._

object UpdateExpressionExamples extends App {
  val path1 = Root("one")(2)
  val path2 = Root("two")

  val set1: SetAction      = path1.set(1)
  val set2: SetAction      = path1.set(path2)
  val set3: SetAction      = path1.setIfNotExists(path2, "v2")
  val set4: SetAction      = path1.setListAppend(List("1"))
  val set5: SetAction      = path1.setListPrepend(List("1"))
  val add: AddAction       = path1.add("V2")
  val remove: RemoveAction = path1.remove
  val delete: DeleteAction = path1.deleteFromSet("v2")

  val ops: UpdateExpression =
    UpdateExpression(
      path1.set(1) +
        path1.set(path2) +
        path1.setIfNotExists(path2, "v2") +
        path1.setListAppend(List("x1", "x2")) +
        path1.setListPrepend(List("x", "x2")) +
        path1.add(1) +
        path1.remove +
        path1.deleteFromSet(1)
    )

  path1.set("v2")
  path1.set(Set("s"))
  path1.set(List("42".toByte))
  path1.set(List(List("41".toByte)))
  path1.set(1)
  path1.set(Set(1))
  path1.set(List("x"))
  DynamoDBQuery.updateItem(TableName("t1"), AttrMap.empty)(set1)

  val pe8: Action.SetAction = path1.set(Map("x" -> "x"))
}
