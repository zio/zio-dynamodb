package zio.dynamodb

import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.json.DynamodbJsonCodec.Encoder
import zio.schema.Schema

package object json {

  /** Adds `toJsonString` and `toJsonStringPretty` to an `AttrMap` (an `Item`). */
  implicit class AttrMapJsonOps(am: AttrMap) {

    /**
     * Renders this item as DynamoDB JSON on one line, for example `{"id":{"S":"a"},"n":{"N":"1"}}`. Currently a nested
     * map is written without the `M` type key that the AWS format uses, and a binary (`B`) or binary set (`BS`) value
     * throws a `scala.NotImplementedError`.
     */
    def toJsonString: String = Encoder.attributeValueToJsonString(am.toAttributeValue)

    /**
     * Renders this item as indented DynamoDB JSON. Like `toJsonString`, it currently writes a nested map without the `M`
     * type key and throws a `scala.NotImplementedError` on a binary value.
     */
    def toJsonStringPretty: String = Encoder.attributeValueToJsonStringPretty(am.toAttributeValue)
  }

  /** Adds `toJsonString` and `toJsonStringPretty` to any value, given a `Schema` for it. */
  implicit class ProductJsonOps[A](a: A) {

    /**
     * Encodes this value with the `Schema[A2]` and renders it as DynamoDB JSON on one line. Choosing a supertype as
     * `A2` selects that type's schema, so for a case of a sealed trait `value.toJsonString[Trait]` includes the
     * discriminator that `put[Trait]` stores. Throws a `java.lang.Exception` with a message that starts with
     * `error encoding` when the value does not encode to an item, for example for an `Int` or a `List[Int]`. A value with a binary
     * field, such as a `Chunk[Byte]`, throws a `scala.NotImplementedError`.
     */
    def toJsonString[A2 >: A](implicit ev: Schema[A2]): String =
      Encoder.attributeValueToJsonString(toItem[A2](a).toAttributeValue)

    /** Like `toJsonString`, but renders indented DynamoDB JSON. */
    def toJsonStringPretty[A2 >: A](implicit ev: Schema[A2]): String =
      Encoder.attributeValueToJsonStringPretty(toItem[A2](a).toAttributeValue)
  }

  /**
   * Parses DynamoDB JSON text into an `AttrMap`. Returns a `Left` holding an `ItemError.DecodingError` when the text is
   * not valid JSON, is not a JSON object (for example a top-level array, number or string), is the empty object `{}`,
   * or contains a binary (`B`) or binary set (`BS`) value, which are not supported yet.
   */
  def parseItem(json: String): Either[DynamoDBError.ItemError, AttrMap] =
    DynamodbJsonCodec.Decoder
      .jsonStringToAttributeValue(json)
      .left
      .map(DynamoDBError.ItemError.DecodingError.apply)
      .flatMap(FromAttributeValue.attrMapFromAttributeValue.fromAttributeValue)

  /**
   * Parses DynamoDB JSON text into an `A` using its `Schema`. Returns a `Left` holding an `ItemError` when the text
   * cannot be parsed (see `parseItem`) or does not match the schema, for example when a field is missing.
   */
  def parse[A: Schema](jsonString: String): Either[DynamoDBError.ItemError, A] =
    parseItem(jsonString).flatMap(fromItem[A])

  private def toItem[A: Schema](a: A): Item =
    FromAttributeValue.attrMapFromAttributeValue
      .fromAttributeValue(AttributeValue.encode(a))
      .getOrElse(throw new Exception(s"error encoding $a"))

  private def fromItem[A: Schema](item: Item): Either[ItemError, A] = {
    val av = ToAttributeValue.attrMapToAttributeValue.toAttributeValue(item)
    av.decode(Schema[A])
  }

}
