package zio.dynamodb

final case class AttrMap(map: Map[String, AttributeValue])
object AttrMap {

  val empty = new AttrMap(Map.empty)

  def apply[A](tuple: (String, A))(implicit A: ToAttributeValue[A]): AttrMap =
    AttrMap(Map(tuple._1 -> A.toAttributeValue(tuple._2)))

  def apply[A, B](t1: (String, A), t2: (String, B))(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B]
  ): AttrMap                                                                 =
    AttrMap(Map(t1._1 -> a.toAttributeValue(t1._2), t2._1 -> b.toAttributeValue(t2._2)))

  def apply[A, B, C](t1: (String, A), t2: (String, B), t3: (String, C))(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C]
  ): AttrMap                                                                 =
    AttrMap(
      Map(t1._1 -> a.toAttributeValue(t1._2), t2._1 -> b.toAttributeValue(t2._2), t3._1 -> c.toAttributeValue(t3._2))
    )
  // TODO: up to arity of 22
}
