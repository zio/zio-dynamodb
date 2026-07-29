package zio.dynamodb

object Utils {
  // move Prelude ops here to maintain zero dependency
  implicit class ListUtils[E, A, B](list: Iterable[A]) {
    def forEach(f: A => Either[E, B]): Either[E, Iterable[B]] = {
      val buf = List.newBuilder[B]
      val it  = list.iterator
      while (it.hasNext)
        f(it.next()) match {
          case Left(e)  => return Left(e)
          case Right(b) => buf += b
        }
      Right(buf.result())
    }

    def reverse: Iterable[A] = {
      var result: List[A] = Nil
      val it              = list.iterator
      while (it.hasNext)
        result = it.next() :: result
      result
    }
  }

}
