package zio.dynamodb
import zio.{ Chunk, ZIO }
import zio.dynamodb.DynamoDBQuery._
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.dynamodb.model.{
  BatchGetItemRequest,
  GetItemRequest,
  KeysAndAttributes,
  PutItemRequest,
  ReturnValue,
  ScanRequest,
  AttributeValue => ZIOAwsAttributeValue,
  ReturnConsumedCapacity => ZIOAwsReturnConsumedCapacity
}
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.stream.Stream

import scala.collection.immutable.{ Map => ScalaMap }

private[dynamodb] final case class DynamoDBExecutorImpl private (dynamoDb: DynamoDb.Service)
    extends DynamoDBExecutor.Service {

  def executeMap[A, B](map: Map[A, B]): ZIO[Any, Exception, B] =
    execute(map.query).map(map.mapper)

  def executeZip[A, B, C](zip: Zip[A, B, C]): ZIO[Any, Exception, C] =
    execute(zip.left).zipWith(execute(zip.right))(zip.zippable.zip)

  def executeConstructor[A](constructor: Constructor[A]): ZIO[Any, Exception, A] =
    constructor match {
      case getItem @ GetItem(_, _, _, _, _)             => doGetItem(getItem)
      case putItem @ PutItem(_, _, _, _, _, _)          => doPutItem(putItem)
      case batchGetItem @ BatchGetItem(_, _, _)         => doBatchGetItem(batchGetItem)
      case scanAll @ ScanAll(_, _, _, _, _, _, _, _, _) => doScanAll(scanAll)
      case _                                            => ???
    }

  override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] =
    atomicQuery match {
      case constructor: Constructor[_] => executeConstructor(constructor)
      case zip @ Zip(_, _, _)          => executeZip(zip)
      case map @ Map(_, _)             => executeMap(map)
    }

  private def doScanAll(scanAll: ScanAll): ZIO[Any, Exception, Stream[Exception, Item]] =
    ZIO.succeed(
      dynamoDb
        .scan(generateScanRequest(scanAll))
        .mapBoth(
          _ => new Exception("boooo"),
          item => toDynamoItem(item)
        )
    )

  private def generateScanRequest(scanAll: ScanAll): ScanRequest =
    ScanRequest(
      tableName = scanAll.tableName.value,
      indexName = ???,
      select = ???,
      scanFilter = ???,
      exclusiveStartKey = ???,
      returnConsumedCapacity = ???,
      totalSegments = ???,
      segment = ???,
      projectionExpression = ???,
      filterExpression = ???,
      expressionAttributeNames = ???,
      expressionAttributeValues = ???,
      consistentRead = ???
    )

  private def doBatchGetItem(batchGetItem: BatchGetItem): ZIO[Any, Exception, BatchGetItem.Response] =
    (for {
      a <- dynamoDb.batchGetItem(generateBatchGetItemRequest(batchGetItem))
      b <- a.responses
    } yield BatchGetItem.Response(
      // TODO(adam): Should we add a + operator to MapOfSet that takes (K, Set[V]) -- yes let's do this
      b.foldLeft(MapOfSet.empty[TableName, Item]) {
        case (acc, (tableName, list)) =>
          list.map(l => (TableName(tableName), toDynamoItem(l))).foldLeft(acc) {
            case (acc, (tableName, item)) =>
              acc + ((tableName, item))
          }
      }
    )).mapError(_ => new Exception("boooo"))

  // TODO(adam): Change our Exception => Throwable and then call toThrowable on the AwsError
  private def doPutItem(putItem: PutItem): ZIO[Any, Exception, Unit] =
    dynamoDb.putItem(generatePutItemRequest(putItem)).unit.mapError(_ => new Exception("abc")) // TODO(adam): Cleanup

  private def doGetItem(getItem: GetItem): ZIO[Any, Exception, Option[Item]] =
    for {
      a <- dynamoDb
             .getItem(generateGetItemRequest(getItem))
             .mapError(_ => new Exception("")) // TODO(adam): This is not an appropriate exception
      c  = a.itemValue.map(toDynamoItem)
    } yield c

  private def toDynamoItem(attrMap: ScalaMap[String, ZIOAwsAttributeValue.ReadOnly]): Item =
    Item(attrMap.view.mapValues(awsAttrValToAttrVal).toMap)

  /*
    let's just combine all of the sets of project expressions for a table to get all of them
    we want this to continue to be a single batch request, we should not be making multiple batch calls

    we'll be returning a little more data possibly -- users may end up just getting the same columns


   */
  private def generateBatchGetItemRequest(batchGetItem: BatchGetItem): BatchGetItemRequest =
    BatchGetItemRequest(
      requestItems = batchGetItem.requestItems.map {
        case (tableName, tableGet) =>
          (tableName.value, generateKeysAndAttributes(tableGet))
      }.toMap,
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(batchGetItem.capacity))
    )

  // TODO(adam): Ask John for assistance on this one?
  private def generateKeysAndAttributes(tableGets: Set[TableGet]): KeysAndAttributes =
    KeysAndAttributes(
      // just end up mapping the (k, v) => (identity, v => v2)
      keys = tableGets.map(_.key.map.view.mapValues(buildAwsAttributeValue).toMap),
      // projectionExpression is really just Option[String]
      projectionExpression = Some(???)
    )

  private def generatePutItemRequest(putItem: PutItem): PutItemRequest =
    PutItemRequest(
      tableName = putItem.tableName.value,
      item = putItem.item.map.view.mapValues(buildAwsAttributeValue).toMap,
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(putItem.capacity)),
      returnItemCollectionMetrics = Some(ReturnItemCollectionMetrics.toZioAws(putItem.itemMetrics)),
      conditionExpression = putItem.conditionExpression.map(_.toString),
      returnValues = Some(buildAwsPutRequestReturnValue(putItem.returnValues))
    )

  private def generateGetItemRequest(getItem: GetItem): GetItemRequest =
    GetItemRequest(
      tableName = getItem.tableName.value,
      key = getItem.key.map.view
        .mapValues(buildAwsAttributeValue)
        .toMap, // TODO(adam): cleanup, just following the types for now

      // attributesToGet is legacy, use projection expression instead
      consistentRead = Some(ConsistencyMode.toBoolean(getItem.consistency)),
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(getItem.capacity)),
      projectionExpression = toOption(getItem.projections).map(
        _.mkString(", ") // TODO(adam): Not sure if this is the best way to combine projection expressions
      ),
      // Do we have support for this?
      // we're going to skip past this for a little while for now and come back to it later
      expressionAttributeNames = None
    )

  // the ZIOAwsAttrVal is just a product encoding of a sum type
  // map the options into something and orElse them
  private def awsAttrValToAttrVal(attributeValue: ZIOAwsAttributeValue.ReadOnly): AttributeValue =
    ???

//  private def buildAwsAttributeMap(getItemResponse: GetItemResponse): Option[AttrMap] =
//    getItemResponse.item.map { i =>
//      AttrMap(i.view.mapValues(aV => awsAttrValToAttrVal(aV)).toMap)
//    }

  private def buildAwsReturnConsumedCapacity(
    returnConsumedCapacity: ReturnConsumedCapacity
  ): ZIOAwsReturnConsumedCapacity =
    // TODO: There is a fourth option for `unknownToSdkVersion`
    returnConsumedCapacity match {
      case ReturnConsumedCapacity.Indexes => ZIOAwsReturnConsumedCapacity.INDEXES
      case ReturnConsumedCapacity.Total   => ZIOAwsReturnConsumedCapacity.TOTAL
      case ReturnConsumedCapacity.None    => ZIOAwsReturnConsumedCapacity.NONE
    }

  private def toOption[A](list: List[A]): Option[::[A]] =
    list match {
      case Nil          => None
      case head :: tail => Some(::(head, tail))
    }

  private def buildAwsPutRequestReturnValue(
    returnValues: ReturnValues
  ): ReturnValue =
    returnValues match {
      case ReturnValues.None       => ReturnValue.NONE
      case ReturnValues.AllOld     => ReturnValue.ALL_OLD
      case ReturnValues.UpdatedOld => ReturnValue.UPDATED_OLD
      case ReturnValues.AllNew     => ReturnValue.ALL_NEW
      case ReturnValues.UpdatedNew => ReturnValue.UPDATED_NEW
    }

  private def buildAwsAttributeValue(
    attributeVal: AttributeValue
  ): ZIOAwsAttributeValue =
    attributeVal match {
      case AttributeValue.Binary(value)    => ZIOAwsAttributeValue(b = Some(Chunk.fromIterable(value)))
      case AttributeValue.BinarySet(value) => ZIOAwsAttributeValue(bs = Some(value.map(Chunk.fromIterable)))
      case AttributeValue.Bool(value)      => ZIOAwsAttributeValue(bool = Some(value))
      case AttributeValue.List(value)      => ZIOAwsAttributeValue(l = Some(value.map(buildAwsAttributeValue)))
      case AttributeValue.Map(value)       =>
        ZIOAwsAttributeValue(m =
          Some(
            value.view.map {
              case (k, v) =>
                (k.value, buildAwsAttributeValue(v))
            }.toMap
          )
        )

      case AttributeValue.Number(value)    => ZIOAwsAttributeValue(n = Some(value.toString()))
      case AttributeValue.NumberSet(value) => ZIOAwsAttributeValue(ns = Some(value.map(_.toString())))
      case AttributeValue.Null             => ZIOAwsAttributeValue(nul = Some(true))
      case AttributeValue.String(value)    => ZIOAwsAttributeValue(s = Some(value))
      case AttributeValue.StringSet(value) => ZIOAwsAttributeValue(ss = Some(value))
    }

}
