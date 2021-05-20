package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{
  BatchGetItem,
  BatchWriteItem,
  CreateTable,
  DeleteItem,
  GetItem,
  PutItem,
  QueryAll,
  QueryPage,
  ScanAll,
  ScanPage,
  UpdateItem
}
import zio.dynamodb.ProjectionExpression.Root
import zio.dynamodb.UpdateExpression.Action.RemoveAction
import zio.stream.ZStream
import zio.{ Chunk, Has, ZIO, ZLayer }

import scala.collection.immutable.{ Map => ScalaMap }

object DynamoDBExecutor {
  type DynamoDBExecutor = Has[Service]

  trait Service {
    def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A]
  }

  //noinspection TypeAnnotation
  object TestData {
    val emptyItem                         = Item(ScalaMap.empty)
    def someItem: Option[Item]            = Some(emptyItem)
    def item(a: String): Item             = Item(ScalaMap(a -> AttributeValue.String(a)))
    def someItem(a: String): Option[Item] = Some(item(a))
    def primaryKey(s: String)             = PrimaryKey(ScalaMap(s -> AttributeValue.String(s)))
    def primaryKey(i: Int)                = PrimaryKey(ScalaMap(s"$i" -> AttributeValue.String(s"$i")))
    val primaryKey1                       = PrimaryKey(ScalaMap("k1" -> AttributeValue.String("k1")))
    val primaryKey2                       = PrimaryKey(ScalaMap("k2" -> AttributeValue.String("k2")))
    val primaryKey3                       = PrimaryKey(ScalaMap("k3" -> AttributeValue.String("k3")))
    val tableName1                        = TableName("T1")
    val tableName2                        = TableName("T2")
    val tableName3                        = TableName("T3")
    val indexName1                        = IndexName("I1")
    def getItem(i: Int)                   = GetItem(key = primaryKey(s"k$i"), tableName = tableName1)
    val getItem1                          = GetItem(key = primaryKey1, tableName = tableName1)
    val getItem2                          = GetItem(key = primaryKey2, tableName = tableName1)
    val getItem3                          = GetItem(key = primaryKey3, tableName = tableName3)
    val item1                             = Item(getItem1.key.value)
    val item2                             = Item(getItem2.key.value)

    val putItem1     = PutItem(tableName = tableName1, item = Item(ScalaMap("k1" -> AttributeValue.String("k1"))))
    val putItem2     = PutItem(tableName = tableName1, item = Item(ScalaMap("k2" -> AttributeValue.String("k2"))))
    val updateItem1  =
      UpdateItem(
        tableName = tableName1,
        primaryKey1,
        UpdateExpression(RemoveAction(Root("top")(1)))
      )
    val deleteItem1  = DeleteItem(tableName = tableName1, key = PrimaryKey(ScalaMap.empty))
    val stream1      = ZStream(emptyItem)
    val scanPage1    = ScanPage(tableName1, indexName1, limit = 10)
    val queryPage1   = QueryPage(tableName1, indexName1, limit = 10)
    val scanAll1     = ScanAll(tableName1, indexName1)
    val queryAll1    = QueryAll(tableName1, indexName1)
    val createTable1 = CreateTable(
      tableName1,
      KeySchema("hashKey", "sortKey"),
      attributeDefinitions = NonEmptySet(AttributeDefinition("attr1", AttributeValueType.String)) + AttributeDefinition(
        "attr2",
        AttributeValueType.Number
      )
    )
  }

  // returns hard coded responses for now
  def test = {
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
            ZIO.succeed(BatchWriteItem.Response())

          case GetItem(key, tableName, projections, readConsistency, capacity)                      =>
            println(s"$key $tableName $projections $readConsistency  $capacity")
            ZIO.some(Item(ScalaMap.empty))

          case PutItem(tableName, item, conditionExpression, capacity, itemMetrics, returnValues)   =>
            println(s"$tableName $item $conditionExpression $capacity $itemMetrics $returnValues")
            ZIO.succeed(())

          case UpdateItem(_, _, _, _, _, _, _)                                                      =>
            ZIO.succeed(())

          case DeleteItem(tableName, key, conditionExpression, capacity, itemMetrics, returnValues) =>
            println(s"$tableName $key $conditionExpression $capacity $itemMetrics $returnValues")
            ZIO.succeed(())

          case ScanPage(
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

          case QueryPage(
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
                provisionedThroughput,
                sseSpecification,
                tags
              ) =>
            println(
              s"$tableName, $keySchema, $attributeDefinitions, $billingMode, $globalSecondaryIndexes, $localSecondaryIndexes, $provisionedThroughput, $sseSpecification, $tags"
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
