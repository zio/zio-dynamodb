package zio.dynamodb

// TODO: is it worth using a general non empty set like this?
final case class NonEmptySet[A](head: A, tail: Set[A] = Set.empty[A]) { self =>
  def +(a: A): NonEmptySet[A] = NonEmptySet(a, self.toSet)
  def toSet: Set[A]           = self.tail + self.head
}
