package zio.dynamodb

import zio.dynamodb.blocks.DummyCodec2.AttributeValue2

final class WrappedMap(
  private val underlying: java.util.Map[String, AttributeValue2],
  cloneFn: java.util.Map[String, AttributeValue2] => java.util.Map[String, AttributeValue2]
) extends scala.collection.immutable.AbstractMap[String, AttributeValue2] {

  override def get(key: String): Option[AttributeValue2] = {
    val v = underlying.get(key)
    if (v eq null) None else Some(v)
  }

  def getNullable(key: String): AttributeValue2 =
    underlying.get(key)

  override def iterator: Iterator[(String, AttributeValue2)] = {
    import scala.jdk.CollectionConverters._
    underlying.entrySet().asScala.iterator.map(e => (e.getKey, e.getValue))
  }

  override def removed(key: String): WrappedMap = {
    val copy: java.util.Map[String, AttributeValue2] = cloneFn(underlying)
    copy.remove(key)
    new WrappedMap(copy, cloneFn)
  }

  override def updated[V1 >: AttributeValue2](key: String, value: V1): WrappedMap = {
    val copy: java.util.Map[String, AttributeValue2] = cloneFn(underlying)
    copy.put(key, value.asInstanceOf[AttributeValue2])
    new WrappedMap(copy, cloneFn)
  }

}

object WrappedMap {
  def emptyHashMap: WrappedMap       =
    new WrappedMap(
      new java.util.HashMap[String, AttributeValue2](),
      m => new java.util.HashMap[String, AttributeValue2](m)
    )
  def emptyLinkedHashMap: WrappedMap =
    new WrappedMap(
      new java.util.LinkedHashMap[String, AttributeValue2](),
      m => new java.util.LinkedHashMap[String, AttributeValue2](m)
    )

}
