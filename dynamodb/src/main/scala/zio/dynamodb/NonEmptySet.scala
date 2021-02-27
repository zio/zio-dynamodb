package zio.dynamodb

// TODO: could we could introduce a MAX elements parameter?
final case class NonEmptySet[A] private (override val head: A, override val tail: Set[A]) extends Iterable[A] {
  self =>
  def +(a: A): NonEmptySet[A]                  = NonEmptySet(a, self.toSet)
  def ++(that: NonEmptySet[A]): NonEmptySet[A] = NonEmptySet(that.head, self.toSet ++ that.tail)

  override def iterator: Iterator[A] =
    new Iterator[A] {
      var nextIndex  = 0
      val xs: Seq[A] = (tail + head).toIndexedSeq

      override def hasNext: Boolean = nextIndex < xs.size

      override def next(): A = {
        val a = xs(nextIndex)
        nextIndex += 1
        a
      }
    }

}
object NonEmptySet {
  def apply[A](head: A): NonEmptySet[A] = NonEmptySet(head, tail = Set.empty)
}
