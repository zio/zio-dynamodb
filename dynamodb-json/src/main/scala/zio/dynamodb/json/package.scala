package zio.dynamodb

import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.json.DynamodbJsonCodec.Encoder
import zio.schema.Schema

package object json {
  def foo: String = "Foo"

  implicit class AttrMapJsonOps(am: AttrMap) {
    def toJsonString: String = Encoder.attributeValueToJsonString(am.toAttributeValue)
  }

  implicit class ProductJsonOps[A](a: A)(implicit schema: Schema[A]) {
    def toJsonString: String = Encoder.attributeValueToJsonString(toItem(a).toAttributeValue)
  }

  private[dynamodb] def toItem[A](a: A)(implicit schema: Schema[A]): Item =
    FromAttributeValue.attrMapFromAttributeValue
      .fromAttributeValue(AttributeValue.encode(a)(schema))
      .getOrElse(throw new Exception(s"error encoding $a"))

  def fromItem[A: Schema](item: Item): Either[ItemError, A] = {
    val av = ToAttributeValue.attrMapToAttributeValue.toAttributeValue(item)
    av.decode(Schema[A])
  }

  def parse(json: String): Either[DynamoDBError.ItemError, AttrMap] =
    DynamodbJsonCodec.Decoder
      .jsonStringToAttributeValue(json)
      .left
      .map(DynamoDBError.ItemError.DecodingError)
      .flatMap(FromAttributeValue.attrMapFromAttributeValue.fromAttributeValue)

}
