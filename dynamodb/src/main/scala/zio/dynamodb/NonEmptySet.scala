package zio.dynamodb

// TODO: could we could introduce a MAX elements parameter?
final case class NonEmptySet[A] private (head: A, tail: Set[A]) { self =>
  def +(a: A): NonEmptySet[A]                  = NonEmptySet(a, self.toSet)
  def ++(that: NonEmptySet[A]): NonEmptySet[A] = NonEmptySet(that.head, self.toSet ++ that.tail)
  def toSet: Set[A]                            = self.tail + self.head
}
object NonEmptySet                                              {
  def apply[A](head: A): NonEmptySet[A] = NonEmptySet(head, tail = Set.empty)
}
