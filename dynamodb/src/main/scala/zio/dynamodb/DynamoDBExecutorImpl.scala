package zio.dynamodb
import zio.{ Chunk, ZIO }
import zio.dynamodb.DynamoDBQuery._
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.dynamodb.model.{
  GetItemRequest,
  GetItemResponse,
  AttributeValue => ZIOAwsAttributeValue,
  ReturnConsumedCapacity => ZIOAwsReturnConsumedCapacity
}

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
      case getItem @ GetItem(_, _, _, _, _) => doGetItem(getItem)
      case _                                => ???
    }

  override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] =
    atomicQuery match {
      case constructor: Constructor[_] => executeConstructor(constructor)
      case zip @ Zip(_, _, _)          => executeZip(zip)
      case map @ Map(_, _)             => executeMap(map)
    }

  private def doGetItem(getItem: GetItem): ZIO[Any, Exception, Option[Item]] =
    for {
      a <- dynamoDb.getItem(generateGetItemRequest(getItem)).mapError(_ => new Exception(""))
      c  = toDynamoItem(a)
      // TODO(adam): I get a map of string -> AttributeValues, what do I do with this to convert it to an A?
    } yield c

  private def toDynamoItem(getItemResponse: GetItemResponse.ReadOnly): Option[Item] =
    getItemResponse.itemValue.map(a => Item(a.view.mapValues(awsAttrValToAttrVal).toMap))

  private def generateGetItemRequest(getItem: GetItem): GetItemRequest =
    // What is the get everything case?
    GetItemRequest(
      tableName = getItem.tableName.value,
      key =
        getItem.key.map.view.mapValues(buildAwsAttributeValue).toMap, // TODO: cleanup, just following the types for now
      // TODO: What is the difference between attributes to get and ProjectionExpression?
      attributesToGet = toOption(getItem.projections.map(_.toString)),
      consistentRead = Some(ConsistencyMode.toBoolean(getItem.consistency)),
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(getItem.capacity)),
      projectionExpression = None,
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
