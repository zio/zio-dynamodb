package zio.dynamodb.examples

import zio.Chunk
import zio.dynamodb.ProjectionExpression.TopLevel
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb.UpdateExpression.Action.{ AddAction, DeleteAction, RemoveAction, SetAction }
import zio.dynamodb.UpdateExpression.SetOperand.{ IfNotExists, ListAppend, ListPrepend, PathOperand, ValueOperand }
import zio.dynamodb._

object UpdateExpressionExamples extends App {
  val path1 = TopLevel("one")(2)
  val path2 = TopLevel("two")
  val list1 = AttributeValue.List(Chunk.empty)
  val list2 = AttributeValue.List(Chunk.empty)

  val set1: SetAction      = SetAction(path1, ValueOperand(AttributeValue.Number(1.0)))
  val set2: SetAction      = SetAction(path1, PathOperand(TopLevel("root")))
  val set3: SetAction      =
    SetAction(path1, IfNotExists(path2, AttributeValue.String("v2")))
  val set4: SetAction      = SetAction(path1, ListAppend(list1))
  val set5: SetAction      = SetAction(path1, ListPrepend(list2))
  val add: AddAction       = AddAction(path1, AttributeValue.String("v2"))
  val remove: RemoveAction = RemoveAction(path1)
  val delete: DeleteAction = DeleteAction(path1, AttributeValue.String("v2"))

  println(
    UpdateExpression(set1) + set2 + add + remove + delete
  )

  val ops: UpdateExpression =
    UpdateExpression(path1.set(BigDecimal(1.0))) +
      path1.set(path2) +
      path1.setIfNotExists(path2, "v2") +
      path1.setListAppend(Chunk("x1", "x2")) +
      path1.setListPrepend(Chunk("x", "x2")) +
      path1.add(BigDecimal(1.0)) +
      path1.remove +
      path1.delete(BigDecimal(1.0))

  /*
  $("one[2]")
  $("foo.bar[9].baz")
   */
  val path3      = TopLevel("one")(2)
  val pe         = path1.set("v2")
  val pe2        = path1.set(Set("s"))
  val pe3        = path1.set(Chunk("s".toByte))
  val pe4        = path1.set(Chunk(Chunk("s".toByte)))
  val pe5        = path1.set(BigDecimal(1.0))
  val pe6        = path1.set(Set(BigDecimal(1.0)))
  val pe7        = path1.set(Chunk("x")) // TODO - see if we can use Iterable
  val updateItem = DynamoDBQuery.updateItem(TableName("t1"), PrimaryKey(Map.empty), pe)

  val x                     = AttributeValue.Map(Map(AttributeValue.String("") -> AttributeValue.String("")))
  val pe8: Action.SetAction = path1.set(Map("x" -> "x"))
}
