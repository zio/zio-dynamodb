package zio.dynamodb

// TODO: Avi - Phase to is to use Scala String as the Key
import zio.dynamodb.AttributeValue.String

final class JMapView(
  private val underlying: java.util.Map[String, AttributeValue],
  cloneFn: java.util.Map[String, AttributeValue] => java.util.Map[String, AttributeValue]
) extends scala.collection.immutable.AbstractMap[String, AttributeValue] {

  override def get(key: String): Option[AttributeValue] = {
    val v = underlying.get(key)
    if (v eq null) None else Some(v)
  }

  def getNullable(key: String): AttributeValue =
    underlying.get(key)

  override def iterator: Iterator[(String, AttributeValue)] = {
    import scala.jdk.CollectionConverters._
    underlying.entrySet().asScala.iterator.map(e => (e.getKey, e.getValue))
  }

  override def removed(key: String): JMapView = {
    val copy: java.util.Map[String, AttributeValue] = cloneFn(underlying)
    copy.remove(key)
    new JMapView(copy, cloneFn)
  }

  override def updated[V1 >: AttributeValue](key: String, value: V1): JMapView = {
    val copy: java.util.Map[String, AttributeValue] = cloneFn(underlying)
    copy.put(key, value.asInstanceOf[AttributeValue])
    new JMapView(copy, cloneFn)
  }

}

object JMapView {
  def emptyHashMap: JMapView       =
    new JMapView(
      new java.util.HashMap[String, AttributeValue](),
      m => new java.util.HashMap[String, AttributeValue](m)
    )
  def emptyLinkedHashMap: JMapView =
    new JMapView(
      new java.util.LinkedHashMap[String, AttributeValue](),
      m => new java.util.LinkedHashMap[String, AttributeValue](m)
    )

}
