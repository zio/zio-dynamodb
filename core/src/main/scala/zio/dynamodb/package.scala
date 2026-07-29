package zio

import zio.dynamodb.DynamoDBError.ItemError.DecodingError

package object dynamodb {
  val PrimaryKey = AttrMap
  type Item                    = AttrMap
  type LastEvaluatedKey        = Option[PrimaryKey]
  type PrimaryKey              = AttrMap
  type FilterExpression[-From] = ConditionExpression[From]

  val Item = AttrMap

  type Encoder[A]  = A => AttributeValue
  type Decoder[+A] = AttributeValue => Either[DecodingError, A]

}
