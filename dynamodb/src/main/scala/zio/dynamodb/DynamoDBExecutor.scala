package zio.dynamodb

import zio.dynamodb.AttributeDefinition.{ attrDefnNumber, attrDefnString }
import zio.dynamodb.DynamoDBQuery.{
  createTable,
  deleteItem,
  getItem,
  putItem,
  queryAll,
  querySome,
  scanAll,
  scanSome,
  updateItem,
  BatchGetItem,
  BatchWriteItem,
  CreateTable,
  DeleteItem,
  GetItem,
  PutItem,
  QueryAll,
  QuerySome,
  ScanAll,
  ScanSome,
  UpdateItem
}
import zio.dynamodb.ProjectionExpression.$
import zio.stream.ZStream
import zio.{ Chunk, Has, ZIO, ZLayer }

import scala.collection.immutable.{ Map => ScalaMap }

//TODO: remove
//object Foo {
//  val m: Map[Item, Item] = Map.empty + (PrimaryKey("a" -> "b") -> Item("a" -> "b"))
//}

object DynamoDBExecutor {
  type DynamoDBExecutor = Has[Service]

  trait Service {
    def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A]
  }

  //noinspection TypeAnnotation
  object TestData {
    val emptyItem                         = Item.empty
    def someItem: Option[Item]            = Some(emptyItem)
    def item(a: String): Item             = Item(a -> a)
    def someItem(a: String): Option[Item] = Some(item(a))
    def primaryKey(s: String)             = PrimaryKey(s -> s)
    def primaryKey(i: Int)                = PrimaryKey(s"$i" -> s"$i")
    val primaryKey1                       = PrimaryKey("k1" -> "k1")
    val primaryKey2                       = PrimaryKey("k2" -> "k2")
    val primaryKey3                       = PrimaryKey("k3" -> "k3")

    def createGetItem(i: Int) = getItem("T1", primaryKey(s"k$i"))
    val getItem1              = getItem("T1", primaryKey1)
    val getItem2              = getItem("T1", primaryKey2)
    val getItem3              = getItem("T3", primaryKey3)
    val item1: Item           = primaryKey1
    val item2: Item           = primaryKey2

    val putItem1     = putItem("T1", item = Item("k1" -> "k1"))
    val putItem2     = putItem("T1", item = Item("k2" -> "k2"))
    val updateItem1  =
      updateItem("T1", PrimaryKey("k1" -> "k1"))(
        $("top[1]").remove
      )
    val deleteItem1  = deleteItem("T1", key = PrimaryKey.empty)
    val stream1      = ZStream(emptyItem)
    val scanPage1    = scanSome("T1", "I1", limit = 10)
    val queryPage1   = querySome("T1", "I1", limit = 10)
    val scanAll1     = scanAll("T1", "I1")
    val queryAll1    = queryAll("T1", "I1")
    val createTable1 = createTable(
      "T1",
      KeySchema("hashKey", "sortKey"),
      BillingMode.provisioned(readCapacityUnit = 10, writeCapacityUnit = 10)
    )(attrDefnString("attr1"), attrDefnNumber("attr2"))
  }

  // returns hard coded responses for now
  val test = {
    import TestData._

    ZLayer.succeed(new Service {
      override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] = {
        val tableName1 = TableName("T1")

        atomicQuery match {

          case BatchGetItem(requestItems, capacity, _)                                              =>
            println(s"$requestItems $capacity")
            // TODO: we could execute in a loop
            val responses = MapOfSet.empty.addAll(tableName1 -> item1, tableName1 -> item2)
            ZIO.succeed(BatchGetItem.Response(responses, ScalaMap.empty))

          case BatchWriteItem(requestItems, capacity, metrics, addList)                             =>
            println(s"$requestItems $capacity $metrics $addList")
            // TODO: we could execute in a loop
            ZIO.succeed(())

          case GetItem(key, tableName, projections, readConsistency, capacity)                      =>
            println(s"$key $tableName $projections $readConsistency  $capacity")
            ZIO.some(Item.empty)

          case PutItem(tableName, item, conditionExpression, capacity, itemMetrics, returnValues)   =>
            println(s"$tableName $item $conditionExpression $capacity $itemMetrics $returnValues")
            ZIO.succeed(())

          case UpdateItem(_, _, _, _, _, _, _)                                                      =>
            ZIO.succeed(())

          case DeleteItem(tableName, key, conditionExpression, capacity, itemMetrics, returnValues) =>
            println(s"$tableName $key $conditionExpression $capacity $itemMetrics $returnValues")
            ZIO.succeed(())

          case ScanSome(
                tableName,
                indexName,
                readConsistency,
                exclusiveStartKey,
                filterExpression,
                limit,
                projections,
                capacity,
                select
              ) =>
            println(
              s"$tableName, $indexName, $readConsistency, $exclusiveStartKey, $filterExpression, $limit, $projections, $capacity, $select"
            )
            ZIO.succeed((Chunk(emptyItem), None))

          case QuerySome(
                tableName,
                indexName,
                readConsistency,
                exclusiveStartKey,
                filterExpression,
                keyConditionExpression,
                limit,
                projections,
                capacity,
                select,
                scanIndexForward
              ) =>
            println(
              s"$tableName, $indexName, $readConsistency, $exclusiveStartKey, $filterExpression, $keyConditionExpression, $limit, $projections, $capacity, $select, $scanIndexForward"
            )
            ZIO.succeed((Chunk(emptyItem), None))

          case ScanAll(
                tableName,
                indexName,
                readConsistency,
                exclusiveStartKey,
                filterExpression,
                projections,
                capacity,
                select
              ) =>
            println(
              s"$tableName, $indexName, $readConsistency, $exclusiveStartKey, $filterExpression, $projections, $capacity, $select"
            )
            ZIO.succeed(stream1)

          case QueryAll(
                tableName,
                indexName,
                readConsistency,
                exclusiveStartKey,
                filterExpression,
                keyConditionExpression,
                projections,
                capacity,
                select,
                scanIndexForward
              ) =>
            println(
              s"$tableName, $indexName, $readConsistency, $exclusiveStartKey, $filterExpression, $keyConditionExpression, $projections, $capacity, $select, $scanIndexForward"
            )
            ZIO.succeed(stream1)

          case CreateTable(
                tableName,
                keySchema,
                attributeDefinitions,
                billingMode,
                globalSecondaryIndexes,
                localSecondaryIndexes,
                sseSpecification,
                tags
              ) =>
            println(
              s"$tableName, $keySchema, $attributeDefinitions, $billingMode, $globalSecondaryIndexes, $localSecondaryIndexes, $sseSpecification, $tags"
            )
            ZIO.succeed(())

          // TODO: remove
          case unknown                                                                              =>
            ZIO.fail(new Exception(s"Constructor $unknown not implemented yet"))
        }
      }

    })
  }
}
