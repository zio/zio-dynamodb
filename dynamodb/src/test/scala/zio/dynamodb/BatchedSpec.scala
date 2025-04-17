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
        decisions.size == 1,
        decisions.head == "single GetItem"
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
        decisions.size == 1,
        decisions.head == "single PutItem"
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
        decisions.size == 1,
        decisions.head == "single DeleteItem"
      )
    },
    test("A GetItem and a DeleteItem do not get batched") {
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
        decisions.size == 2,
        decisions.contains("single GetItem") == true,
        decisions.contains("single DeleteItem") == true
      )
    },
    test("A GetItem and a PutItem do not get batched") {
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
        decisions.size == 2,
        decisions.contains("single GetItem") == true,
        decisions.contains("single PutItem") == true
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
        decisions.size == 1,
        decisions.head == "multiple GetItem's"
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
        decisions.size == 1,
        decisions.head == "multiple PutItem/DeleteItem"
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
        decisions.size == 2,
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
        decisions.size == 2,
        decisions.contains("PutItem has a condition expression") == true,
        decisions.contains("DeleteItem has a condition expression") == true
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
        decisions.size == 2,
        decisions.contains("PutItem has a return value other than None") == true,
        decisions.contains("DeleteItem has a return value other than None") == true
      )
    }
  )

}
