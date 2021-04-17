package zio.dynamodb.examples

import zio.Chunk
import zio.dynamodb.ProjectionExpression.TopLevel
import zio.dynamodb.UpdateExpression.Action.{ AddAction, DeleteAction, RemoveAction, SetAction }
import zio.dynamodb.UpdateExpression.SetOperand.{ IfNotExists, ListAppend, PathOperand, ValueOperand }
import zio.dynamodb.{ AttributeValue, UpdateExpression }

object UpdateExpressionExamples extends App {
  val path1 = TopLevel("one")(2)
  val path2 = TopLevel("two")
  val list1 = AttributeValue.List(Chunk.empty)
  val list2 = AttributeValue.List(Chunk.empty)

  val set1: SetAction      = SetAction(path1, ValueOperand(AttributeValue.Number(1.0)))
  val set2: SetAction      = SetAction(path1, PathOperand(TopLevel("root")))
  val set3: SetAction      =
    SetAction(path1, IfNotExists(path2, AttributeValue.String("v2")))
  val set4: SetAction      = SetAction(path1, ListAppend(list1, list2))
  val add: AddAction       = AddAction(path1, AttributeValue.String("v2"))
  val remove: RemoveAction = RemoveAction(path1)
  val delete: DeleteAction = DeleteAction(path1, AttributeValue.String("v2"))

  println(
    UpdateExpression(set1) + set2 + add + remove + delete
  )

  val set5: SetAction = path1.set(AttributeValue.Number(1.0))
  val set6: SetAction = path1.set(path2)
  val set7: SetAction = path1.setIfNotExists(path2, AttributeValue.String("v2"))
  val set8: SetAction = path1.setListAppend(list1, list2)
  val add2            = path1.add(AttributeValue.Number(1.0))
  val remove2         = path1.remove
  val delete2         = path1.delete(AttributeValue.Number(1.0))

  val ops = UpdateExpression(set5) + set6 + set7 + add2 + remove2 + delete2
}
