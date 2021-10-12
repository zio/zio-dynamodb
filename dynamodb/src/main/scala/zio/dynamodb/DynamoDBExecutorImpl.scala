package zio.dynamodb
import zio.{ Chunk, ZIO }
import zio.dynamodb.DynamoDBQuery._
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.dynamodb.model.{
  BatchGetItemRequest,
  GetItemRequest,
  GetItemResponse,
  KeysAndAttributes,
  PutItemRequest,
  ReturnValue,
  AttributeValue => ZIOAwsAttributeValue,
  ReturnConsumedCapacity => ZIOAwsReturnConsumedCapacity
}
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import scala.collection.immutable.{ Map => ScalaMap }

private[dynamodb] final case class DynamoDBExecutorImpl private (dynamoDb: DynamoDb.Service)
    extends DynamoDBExecutor.Service {

  def executeMap[A, B](map: Map[A, B]): ZIO[Any, Exception, B] =
    execute(map.query).map(map.mapper)

  def executeZip[A, B, C](zip: Zip[A, B, C]): ZIO[Any, Exception, C] =
    execute(zip.left).zipWith(execute(zip.right))(zip.zippable.zip)

  // TODO(adam): I'm confused about this. A `GetItem` extends a `Constructor[Option[Item]]`.
  //    Should the getItem match not return an Option[Item]???
  //    Is this just a problem with IntelliJ not knowing about the types?
  def executeConstructor[A](constructor: Constructor[A]): ZIO[Any, Exception, A] =
    constructor match {
      case getItem @ GetItem(_, _, _, _, _)     => doGetItem(getItem)
      case putItem @ PutItem(_, _, _, _, _, _)  => doPutItem(putItem)
      case batchGetItem @ BatchGetItem(_, _, _) => doBatchGetItem(batchGetItem)
      case _                                    => ???
    }

  override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] =
    atomicQuery match {
      case constructor: Constructor[_] => executeConstructor(constructor)
      case zip @ Zip(_, _, _)          => executeZip(zip)
      case map @ Map(_, _)             => executeMap(map)
    }

  private def doBatchGetItem(batchGetItem: BatchGetItem): ZIO[Any, Exception, BatchGetItem.Response] =
    for {
      a <- dynamoDb.batchGetItem(generateBatchGetItemRequest(batchGetItem)).mapError(_ => new Exception("boooo"))
      b <- a.responses.mapError(_ => new Exception("boooo"))
    } yield BatchGetItem.Response(MapOfSet(b.view.mapValues(a => a.map(toDynamoItem).toSet).toMap))

  private def doPutItem(putItem: PutItem): ZIO[Any, Exception, Unit] =
    dynamoDb.putItem(generatePutItemRequest(putItem)).unit.mapError(e => new Exception("abc"))

  private def doGetItem(getItem: GetItem): ZIO[Any, Exception, Option[Item]] =
    for {
      a <- dynamoDb
             .getItem(generateGetItemRequest(getItem))
             .mapError(_ => new Exception("")) // TODO(adam): This is not an appropriate exception
      c  = a.itemValue.map(toDynamoItem)
    } yield c

  private def toDynamoItem(attrMap: ScalaMap[String, ZIOAwsAttributeValue.ReadOnly]): Item =
    Item(attrMap.view.mapValues(awsAttrValToAttrVal).toMap)

  private def generateBatchGetItemRequest(batchGetItem: BatchGetItem): BatchGetItemRequest =
    BatchGetItemRequest(
      requestItems = batchGetItem.requestItems.map {
        case (tableName, tableGet) =>
          (tableName.value, generateKeysAndAttributes(tableGet))
      }.toMap,
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(batchGetItem.capacity))
    )

  private def generateKeysAndAttributes(tableGets: Set[TableGet]): KeysAndAttributes =
    KeysAndAttributes(
      keys = tableGets.map(_.key.map.view.mapValues(buildAwsAttributeValue).toMap),
      attributesToGet = ??? // TODO(adam): It looks like here we have an issue with the types not aligning
      /* Our TableGet has a projections (attributesToGet) but the KeysAndAttributes expects one projection rather than one per
       *
       */
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
      expressionAttributeNames = None
    )

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
