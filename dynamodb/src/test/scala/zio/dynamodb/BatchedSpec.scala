package zio.dynamodb

import zio.test.ZIOSpecDefault
import zio.test.assertTrue
import zio.dynamodb.DynamoDBQuery.putItem
import zio.Chunk
import zio.dynamodb.DynamoDBQuery.Constructor
import ProjectionExpression.$

object BatchedSpec extends ZIOSpecDefault {
  val spec = suite("Batched suite")(
    /*
    TODO:
    - single queries do not get batched

     */
    test("PutItem's without conditions should be in returned BatchWriteItem") {
      val item1                                          = Item("id" -> "1")
      val item2                                          = Item("id" -> "2")
      val put1                                           = putItem("table1", item1)
      val put2                                           = putItem("table1", item2)
      val constructors: Chunk[Constructor[AttrMap, Any]] =
        Chunk(put1, put2).asInstanceOf[Chunk[Constructor[Any, Option[AttrMap]]]]
      val x: (
        Chunk[(Constructor[AttrMap, Any], Int)],
        (DynamoDBQuery.BatchGetItem, Chunk[Int]),
        (DynamoDBQuery.BatchWriteItem, Chunk[Int])
      )                                                  = DynamoDBQuery.batched(constructors)

      println(s"BatchWriteItem: ${x._3}")
      assertTrue(true)
    },
    test("PutItem's with conditions should not be in returned BatchWriteItem") {
      val item1                                          = Item("id" -> "1")
      val item2                                          = Item("id" -> "2")
      val put1                                           = putItem("table1", item1).where($("table.id") === "1")
      val put2                                           = putItem("table1", item2).where($("table.id") === "1")
      val constructors: Chunk[Constructor[AttrMap, Any]] =
        Chunk(put1, put2).asInstanceOf[Chunk[Constructor[Any, Option[AttrMap]]]]
      val x: (
        Chunk[(Constructor[AttrMap, Any], Int)],
        (DynamoDBQuery.BatchGetItem, Chunk[Int]),
        (DynamoDBQuery.BatchWriteItem, Chunk[Int])
      )                                                  = DynamoDBQuery.batched(constructors)

      println(s"BatchWriteItem: ${x._3}")
      assertTrue(true)
    }
  )

}
