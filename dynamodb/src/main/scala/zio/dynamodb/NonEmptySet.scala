package zio.dynamodb

// TODO: maybe we could introduce a MAX elements restriction at the type level?
final case class NonEmptySet[A] private (override val head: A, override val tail: Set[A]) extends Iterable[A] {
  self =>
  def +(a: A): NonEmptySet[A]                  = NonEmptySet(a, self.toSet)
  def ++(that: NonEmptySet[A]): NonEmptySet[A] = NonEmptySet(that.head, self.toSet ++ that.tail)

  override def iterator: Iterator[A] = (tail + head).iterator

}
object NonEmptySet {
  def apply[A](head: A): NonEmptySet[A] = NonEmptySet(head, tail = Set.empty)
}
