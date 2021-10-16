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
  ReturnConsumedCapacity => ZIOAwsReturnConsumedCapacity,
  Select => ZIOAwsSelect
}
import zio.dynamodb.ConsistencyMode.toBoolean
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.stream.Stream

import scala.collection.immutable.{ Map => ScalaMap }

private[dynamodb] final case class DynamoDBExecutorImpl private (dynamoDb: DynamoDb.Service) extends DynamoDBExecutor {

  def executeMap[A, B](map: Map[A, B]): ZIO[Any, Throwable, B] =
    execute(map.query).map(map.mapper)

  def executeZip[A, B, C](zip: Zip[A, B, C]): ZIO[Any, Throwable, C] =
    execute(zip.left).zipWith(execute(zip.right))(zip.zippable.zip)

  def executeConstructor[A](constructor: Constructor[A]): ZIO[Any, Throwable, A] =
    constructor match {
      case getItem @ GetItem(_, _, _, _, _)             => doGetItem(getItem)
      case putItem @ PutItem(_, _, _, _, _, _)          => doPutItem(putItem)
      case batchGetItem @ BatchGetItem(_, _, _)         => doBatchGetItem(batchGetItem)
      case scanAll @ ScanAll(_, _, _, _, _, _, _, _, _) => doScanAll(scanAll)
      case _                                            => ???
    }

  override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Throwable, A] =
    atomicQuery match {
      case constructor: Constructor[_] => executeConstructor(constructor)
      case zip @ Zip(_, _, _)          => executeZip(zip)
      case map @ Map(_, _)             => executeMap(map)
    }

  private def doScanAll(scanAll: ScanAll): ZIO[Any, Throwable, Stream[Throwable, Item]] =
    ZIO.succeed(
      dynamoDb
        .scan(generateScanRequest(scanAll))
        .mapBoth(
          awsError => awsError.toThrowable,
          item => toDynamoItem(item)
        )
    )

  private def selectToZioAwsSelect(select: Select): ZIOAwsSelect =
    select match {
      case Select.Count                  => ZIOAwsSelect.COUNT
      case Select.AllAttributes          => ZIOAwsSelect.ALL_ATTRIBUTES
      case Select.AllProjectedAttributes => ZIOAwsSelect.ALL_PROJECTED_ATTRIBUTES
      case Select.SpecificAttributes     => ZIOAwsSelect.SPECIFIC_ATTRIBUTES
    }

  private def generateScanRequest(scanAll: ScanAll): ScanRequest =
    ScanRequest(
      tableName = scanAll.tableName.value,
      indexName = Some(scanAll.indexName.value),
      select = scanAll.select.map(selectToZioAwsSelect),
      exclusiveStartKey = scanAll.exclusiveStartKey.map(m => attrMapToAwsAttrMap(m.map)),
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(scanAll.capacity)),
      // Looks like we're not currently supporting segments?
      totalSegments = None,
      segment = None,
      limit = scanAll.limit,
      projectionExpression = toOption(scanAll.projections).map(_.mkString(", ")),
      filterExpression = scanAll.filterExpression.map(filterExpression => filterExpression.render()),
      consistentRead = Some(toBoolean(scanAll.consistency))
    )

  private def doBatchGetItem(batchGetItem: BatchGetItem): ZIO[Any, Throwable, BatchGetItem.Response] =
    (for {
      a <- dynamoDb.batchGetItem(generateBatchGetItemRequest(batchGetItem))
      b <- a.responses
    } yield BatchGetItem.Response(
      b.foldLeft(MapOfSet.empty[TableName, Item]) {
        case (acc, (tableName, list)) => acc ++ ((TableName(tableName), list.map(toDynamoItem)))
      }
    )).mapError(_.toThrowable)

  private def doPutItem(putItem: PutItem): ZIO[Any, Throwable, Unit] =
    dynamoDb.putItem(generatePutItemRequest(putItem)).unit.mapError(_.toThrowable)

  private def doGetItem(getItem: GetItem): ZIO[Any, Throwable, Option[Item]] =
    for {
      a <- dynamoDb
             .getItem(generateGetItemRequest(getItem))
             .mapError(_.toThrowable)
      c  = a.itemValue.map(toDynamoItem)
    } yield c

  private def toDynamoItem(attrMap: ScalaMap[String, ZIOAwsAttributeValue.ReadOnly]): Item =
    Item(attrMap.flatMap { case (k, v) => awsAttrValToAttrVal(v).map(attrVal => (k, attrVal)) })

  private def generateBatchGetItemRequest(batchGetItem: BatchGetItem): BatchGetItemRequest =
    BatchGetItemRequest(
      requestItems = batchGetItem.requestItems.map {
        case (tableName, tableGet) =>
          (tableName.value, generateKeysAndAttributes(tableGet))
      }.toMap,
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(batchGetItem.capacity))
    )

  // Our TableGet is more powerful than zio-aws's batchGet. We can get different projections for the same table where zio-aws cannot
  //    We're going to combine all projection expressions for a table and possibly return more data than the user is requesting
  //      but at the benefit of not doing multiple batches
  private def generateKeysAndAttributes(tableGets: Set[TableGet]): KeysAndAttributes = {
    val setOfProjectionExpressions = tableGets.flatMap(_.projections.toSet)
    KeysAndAttributes(
      keys = tableGets.map(_.key.map.map { case (k, v) => (k, buildAwsAttributeValue(v)) }),
      projectionExpression = Some(setOfProjectionExpressions.mkString(", "))
    )
  }

  private def generatePutItemRequest(putItem: PutItem): PutItemRequest =
    PutItemRequest(
      tableName = putItem.tableName.value,
      item = attrMapToAwsAttrMap(putItem.item.map),
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(putItem.capacity)),
      returnItemCollectionMetrics = Some(ReturnItemCollectionMetrics.toZioAws(putItem.itemMetrics)),
      conditionExpression = putItem.conditionExpression.map(_.toString),
      returnValues = Some(buildAwsPutRequestReturnValue(putItem.returnValues))
    )

  private def attrMapToAwsAttrMap(attrMap: ScalaMap[String, AttributeValue]): ScalaMap[String, ZIOAwsAttributeValue] =
    attrMap.map { case (k, v) => (k, buildAwsAttributeValue(v)) }

  private def generateGetItemRequest(getItem: GetItem): GetItemRequest                                               =
    GetItemRequest(
      tableName = getItem.tableName.value,
      key = getItem.key.map.map { case (k, v) => (k, buildAwsAttributeValue(v)) },
      consistentRead = Some(ConsistencyMode.toBoolean(getItem.consistency)),
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(getItem.capacity)),
      projectionExpression = toOption(getItem.projections).map(
        _.mkString(", ") // TODO(adam): Not sure if this is the best way to combine projection expressions
      )
    )

//  private def awsAttrValToZioAttrVal(
//    attributeValue: ZIOAwsAttributeValue.ReadOnly
//  ): ZIO[Any, Throwable, AttributeValue] =
//    attributeValue.s
//      .map(AttributeValue.String)
//      .orElse(attributeValue.n.map(n => AttributeValue.Number(BigDecimal(n))))
//      .orElse(attributeValue.b.map(AttributeValue.Binary))
//      .orElse(attributeValue.ss.map(ss => AttributeValue.StringSet(ss.toSet)))
//      .orElse(attributeValue.ns.map(ns => AttributeValue.NumberSet(ns.map(BigDecimal(_)).toSet)))
//      .orElse(attributeValue.bs.map(bs => AttributeValue.BinarySet(bs.toSet)))
//      .orElse(
//        attributeValue.m.map(m =>
//          AttributeValue.Map(
//            m.flatMap { case (k, v) => awsAttrValToAttrVal(v).map(attrVal => (AttributeValue.String(k), attrVal)) }
//          )
//        )
//      )
//      .orElse(attributeValue.l.map(l => AttributeValue.List(l.flatMap(awsAttrValToAttrVal))))
//      .orElse(attributeValue.nul.map(_ => AttributeValue.Null))
//      .orElse(attributeValue.bool.map(AttributeValue.Bool))
//      .mapError(_.toThrowable)

  private def awsAttrValToAttrVal(attributeValue: ZIOAwsAttributeValue.ReadOnly): Option[AttributeValue] =
    attributeValue.sValue
      .map(AttributeValue.String)
      .orElse(
        attributeValue.nValue.map(n => AttributeValue.Number(BigDecimal(n)))
      )   // TODO(adam): Does the BigDecimal need a try wrapper?
      .orElse(
        attributeValue.bValue.map(b => AttributeValue.Binary(b))
      )
      .orElse(
        attributeValue.ssValue.map(s =>
          AttributeValue.StringSet(s.toSet)
        ) // TODO(adam): Is this `toSet` actually safe to do?
      )
      .orElse(
        attributeValue.nsValue.map(ns =>
          AttributeValue.NumberSet(ns.map(BigDecimal(_)).toSet)
        ) // TODO(adam): Wrap in try?
      )
      .orElse(
        attributeValue.bsValue.map(bs => AttributeValue.BinarySet(bs.toSet))
      )
      .orElse(
        attributeValue.mValue.map(m =>
          AttributeValue.Map(
            m.flatMap {
              case (k, v) =>
                awsAttrValToAttrVal(v).map(attrVal => (AttributeValue.String(k), attrVal))
            }
          )
        )
      )
      .orElse(attributeValue.lValue.map(l => AttributeValue.List(l.flatMap(awsAttrValToAttrVal))))
      .orElse(attributeValue.nulValue.map(_ => AttributeValue.Null))
      .orElse(attributeValue.boolValue.map(AttributeValue.Bool))

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
        ZIOAwsAttributeValue(m = Some(value.map {
          case (k, v) => (k.value, buildAwsAttributeValue(v))
        }.toMap)) // TODO(adam): Why does this require a toMap?
      case AttributeValue.Number(value)    => ZIOAwsAttributeValue(n = Some(value.toString()))
      case AttributeValue.NumberSet(value) => ZIOAwsAttributeValue(ns = Some(value.map(_.toString())))
      case AttributeValue.Null             => ZIOAwsAttributeValue(nul = Some(true))
      case AttributeValue.String(value)    => ZIOAwsAttributeValue(s = Some(value))
      case AttributeValue.StringSet(value) => ZIOAwsAttributeValue(ss = Some(value))
    }

}
