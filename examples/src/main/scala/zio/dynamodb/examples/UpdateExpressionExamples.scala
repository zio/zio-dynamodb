package zio.dynamodb.examples

import zio.Chunk
import zio.dynamodb.ProjectionExpression.TopLevel
import zio.dynamodb.UpdateExpression.Action.{ AddAction, DeleteAction, RemoveAction, SetAction }
import zio.dynamodb.UpdateExpression.SetOperand.{ IfNotExists, ListAppend, PathOperand, ValueOperand }
import zio.dynamodb.{ AttributeValue, UpdateExpression }

object UpdateExpressionExamples extends App {
  val path1 = TopLevel("one")(2)
  val path2 = TopLevel("two")

  val set1: SetAction      = SetAction(path1, ValueOperand(AttributeValue.Number(1.0)))
  val set2: SetAction      = SetAction(path1, PathOperand(TopLevel("root")))
  val set3: SetAction      =
    SetAction(path1, IfNotExists(path2, AttributeValue.String("v2")))
  val set4: SetAction      =
    SetAction(path1, ListAppend(AttributeValue.List(Chunk.empty), AttributeValue.List(Chunk.empty)))
  val add: AddAction       = AddAction(path1, AttributeValue.String("v2"))
  val remove: RemoveAction = RemoveAction(path1)
  val delete: DeleteAction = DeleteAction(path1, AttributeValue.String("v2"))

  println(
    UpdateExpression(set1) + set2 + add + remove + delete
  )

  /*
  path.set(AttributeValue.Number(1.0))
  path.set(path2)
  path.setIfNotExists(path2, AttributeValue.Number(1.0))
  path.setListAppend(list1, list2)
  path.add(AttributeValue.Number(1.0))
  path.remove(path2)
  path.remove(path2)
   */
}
