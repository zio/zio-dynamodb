package zio.dynamodb

import zio.test.ZIOSpecDefault
import zio.test.assertTrue
import zio.dynamodb.DynamoDBQuery.{ deleteItem, getItem, putItem }
import zio.Chunk
import zio.dynamodb.DynamoDBQuery.Constructor
import ProjectionExpression.$

object BatchedSpec extends ZIOSpecDefault {
  val item1 = Item("id" -> "1")
  val item2 = Item("id" -> "2")
  val spec  = suite("Batched suite")(
    test("Single GetItem queries do not get batched") {
      val get1                                           = getItem("table1", item1)
      val constructors: Chunk[Constructor[AttrMap, Any]] =
        Chunk(get1).asInstanceOf[Chunk[Constructor[Any, Option[AttrMap]]]]
      val (
        nonBatched: Chunk[(Constructor[AttrMap, Any], Int)],
        batchGetItem: (DynamoDBQuery.BatchGetItem, Chunk[Int]),
        batchWriteItem: (DynamoDBQuery.BatchWriteItem, Chunk[Int]),
        decisions
      )                                                  = DynamoDBQuery.batched(constructors)

      assertTrue(
        nonBatched.size == 1,
        batchGetItem._1.requestItems.isEmpty,
        batchWriteItem._1.requestItems.isEmpty,
        decisions == Set("single GetItem")
      )
    },
    test("Single PutItem queries do not get batched") {
      val put1                                           = putItem("table1", item1)
      val constructors: Chunk[Constructor[AttrMap, Any]] =
        Chunk(put1).asInstanceOf[Chunk[Constructor[Any, Option[AttrMap]]]]
      val (
        nonBatched: Chunk[(Constructor[AttrMap, Any], Int)],
        batchGetItem: (DynamoDBQuery.BatchGetItem, Chunk[Int]),
        batchWriteItem: (DynamoDBQuery.BatchWriteItem, Chunk[Int]),
        decisions
      )                                                  = DynamoDBQuery.batched(constructors)

      assertTrue(
        nonBatched.size == 1,
        batchGetItem._1.requestItems.isEmpty,
        batchWriteItem._1.requestItems.isEmpty,
        decisions == Set("single PutItem")
      )
    },
    test("Single DeleteItem queries do not get batched") {
      val delete1                                        = deleteItem("table1", item1)
      val constructors: Chunk[Constructor[AttrMap, Any]] =
        Chunk(delete1).asInstanceOf[Chunk[Constructor[Any, Option[AttrMap]]]]
      val (
        nonBatched: Chunk[(Constructor[AttrMap, Any], Int)],
        batchGetItem: (DynamoDBQuery.BatchGetItem, Chunk[Int]),
        batchWriteItem: (DynamoDBQuery.BatchWriteItem, Chunk[Int]),
        decisions
      )                                                  = DynamoDBQuery.batched(constructors)

      assertTrue(
        nonBatched.size == 1,
        batchGetItem._1.requestItems.isEmpty,
        batchWriteItem._1.requestItems.isEmpty,
        decisions == Set("single DeleteItem")
      )
    },
    test("A single GetItem and a single DeleteItem do not get batched") {
      val get1                                           = getItem("table1", item1)
      val delete1                                        = deleteItem("table1", item2)
      val constructors: Chunk[Constructor[AttrMap, Any]] =
        Chunk(get1, delete1).asInstanceOf[Chunk[Constructor[Any, Option[AttrMap]]]]
      val (
        nonBatched: Chunk[(Constructor[AttrMap, Any], Int)],
        batchGetItem: (DynamoDBQuery.BatchGetItem, Chunk[Int]),
        batchWriteItem: (DynamoDBQuery.BatchWriteItem, Chunk[Int]),
        decisions
      )                                                  = DynamoDBQuery.batched(constructors)

      assertTrue(
        nonBatched.size == 2,
        batchGetItem._1.requestItems.size == 0,
        batchWriteItem._1.requestItems.size == 0,
        decisions == Set("single GetItem", "single DeleteItem")
      )
    },
    test("A single GetItem and a single PutItem do not get batched") {
      val get1                                           = getItem("table1", item1)
      val put1                                           = putItem("table1", item2)
      val constructors: Chunk[Constructor[AttrMap, Any]] =
        Chunk(get1, put1).asInstanceOf[Chunk[Constructor[Any, Option[AttrMap]]]]
      val (
        nonBatched: Chunk[(Constructor[AttrMap, Any], Int)],
        batchGetItem: (DynamoDBQuery.BatchGetItem, Chunk[Int]),
        batchWriteItem: (DynamoDBQuery.BatchWriteItem, Chunk[Int]),
        decisions
      )                                                  = DynamoDBQuery.batched(constructors)

      assertTrue(
        nonBatched.size == 2,
        batchGetItem._1.requestItems.size == 0,
        batchWriteItem._1.requestItems.size == 0,
        decisions == Set("single GetItem", "single PutItem")
      )
    },
    test("Multiple GetItems should be batched") {
      val get1                                           = getItem("table1", item1)
      val get2                                           = getItem("table1", item2)
      val constructors: Chunk[Constructor[AttrMap, Any]] =
        Chunk(get1, get2).asInstanceOf[Chunk[Constructor[Any, Option[AttrMap]]]]
      val (
        nonBatched: Chunk[(Constructor[AttrMap, Any], Int)],
        batchGetItem: (DynamoDBQuery.BatchGetItem, Chunk[Int]),
        batchWriteItem: (DynamoDBQuery.BatchWriteItem, Chunk[Int]),
        decisions
      )                                                  = DynamoDBQuery.batched(constructors)

      assertTrue(
        nonBatched.isEmpty,
        batchGetItem._1.requestItems.size == 1,
        batchGetItem._1.requestItems.get(TableName("table1")).get.keysSet.size == 2,
        batchWriteItem._1.requestItems.isEmpty,
        decisions == Set("multiple GetItem's")
      )
    },
    test("A PutItem and DeleteItem should be batched") {
      val put1                                           = putItem("table1", item1)
      val delete1                                        = deleteItem("table1", item2)
      val constructors: Chunk[Constructor[AttrMap, Any]] =
        Chunk(put1, delete1).asInstanceOf[Chunk[Constructor[Any, Option[AttrMap]]]]
      val (
        nonBatched: Chunk[(Constructor[AttrMap, Any], Int)],
        batchGetItem: (DynamoDBQuery.BatchGetItem, Chunk[Int]),
        batchWriteItem: (DynamoDBQuery.BatchWriteItem, Chunk[Int]),
        decisions
      )                                                  = DynamoDBQuery.batched(constructors)

      assertTrue(
        nonBatched.isEmpty,
        batchGetItem._1.requestItems.isEmpty,
        batchWriteItem._1.requestItems.size == 1,
        batchWriteItem._1.requestItems.get(TableName("table1")).get.size == 2,
        decisions == Set("multiple PutItem/DeleteItem")
      )
    },
    test("Multiple GetItems and multiple WriteItems (Put or Delete) should be batched") {
      val get1    = getItem("table1", item1)
      val get2    = getItem("table1", item2)
      val put1    = putItem("table1", item1)
      val delete1 = deleteItem("table1", item2)

      val constructors: Chunk[Constructor[AttrMap, Any]] =
        Chunk(get1, get2, put1, delete1).asInstanceOf[Chunk[Constructor[Any, Option[AttrMap]]]]
      val (
        nonBatched: Chunk[(Constructor[AttrMap, Any], Int)],
        batchGetItem: (DynamoDBQuery.BatchGetItem, Chunk[Int]),
        batchWriteItem: (DynamoDBQuery.BatchWriteItem, Chunk[Int]),
        decisions
      )                                                  = DynamoDBQuery.batched(constructors)

      assertTrue(
        nonBatched.isEmpty,
        batchGetItem._1.requestItems.size == 1,
        batchGetItem._1.requestItems.get(TableName("table1")).get.keysSet.size == 2,
        batchWriteItem._1.requestItems.size == 1,
        decisions == Set("multiple GetItem's", "multiple PutItem/DeleteItem")
      )
    },
    test("Put/Delete Items with conditions should not be batched") {
      val put1                                           = putItem("table1", item1).where($("table.id") === "1")
      val delete1                                        = deleteItem("table1", item2).where($("table.id") === "2")
      val constructors: Chunk[Constructor[AttrMap, Any]] =
        Chunk(put1, delete1).asInstanceOf[Chunk[Constructor[Any, Option[AttrMap]]]]
      val (
        nonBatched: Chunk[(Constructor[AttrMap, Any], Int)],
        batchGetItem: (DynamoDBQuery.BatchGetItem, Chunk[Int]),
        batchWriteItem: (DynamoDBQuery.BatchWriteItem, Chunk[Int]),
        decisions
      )                                                  = DynamoDBQuery.batched(constructors)

      assertTrue(
        nonBatched.size == 2,
        batchGetItem._1.requestItems.isEmpty,
        batchWriteItem._1.requestItems.isEmpty,
        decisions == Set("PutItem has a condition expression", "DeleteItem has a condition expression")
      )
    },
    test("Put/Delete Items with return values other than ReturnValues.None should not be batched") {
      val put1                                           = putItem("table1", item1).returns(ReturnValues.AllNew)
      val delete1                                        = deleteItem("table1", item2).returns(ReturnValues.AllOld)
      val constructors: Chunk[Constructor[AttrMap, Any]] =
        Chunk(put1, delete1).asInstanceOf[Chunk[Constructor[Any, Option[AttrMap]]]]
      val (
        nonBatched: Chunk[(Constructor[AttrMap, Any], Int)],
        batchGetItem: (DynamoDBQuery.BatchGetItem, Chunk[Int]),
        batchWriteItem: (DynamoDBQuery.BatchWriteItem, Chunk[Int]),
        decisions
      )                                                  = DynamoDBQuery.batched(constructors)

      assertTrue(
        nonBatched.size == 2,
        batchGetItem._1.requestItems.isEmpty,
        batchWriteItem._1.requestItems.isEmpty,
        decisions == Set("PutItem has a return value other than None", "DeleteItem has a return value other than None")
      )
    }
  )

}
