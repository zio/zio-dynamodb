package zio.dynamodb

final case class AliasMapRender[+A](
  render: AliasMap => (AliasMap, A)
) { self =>

  def map[B](f: A => B): AliasMapRender[B] =
    AliasMapRender { aliasMap =>
      val (am, a) = self.render(aliasMap)
      // REVIEW(john): I feel like the need for this ++ here is a flag that I am missing a conbinator(is this the right word?) or have a bad implementation
      (am ++ aliasMap, f(a))
    }

  def flatMap[B](f: A => AliasMapRender[B]): AliasMapRender[B] =
    AliasMapRender { aliasMap =>
      val (am, a) = self.render(aliasMap)
      // REVIEW(john): I feel like the need for this ++ here is a flag that I am missing a conbinator(is this the right word?) or have a bad implementation
      f(a).render(am ++ aliasMap)
    }

  def zipWith[B, C](that: AliasMapRender[B])(f: (A, B) => C): AliasMapRender[C] =
    for {
      a <- self
      b <- that
    } yield f(a, b)

  def zipRight[B](that: AliasMapRender[B]): AliasMapRender[B] =
    self.flatMap(_ => that)

}

object AliasMapRender {
  def getOrInsert(entry: AttributeValue): AliasMapRender[String] =
    AliasMapRender { aliasMap =>
      aliasMap.getOrInsert(entry)
    }

  def empty = AliasMapRender.setMap(AliasMap.empty)

  def succeed[A](a: => A): AliasMapRender[A] = AliasMapRender(aliasMap => (aliasMap, a))

  val getMap: AliasMapRender[AliasMap] = AliasMapRender(aliasMap => (aliasMap, aliasMap))

  def setMap(aliasMap: AliasMap): AliasMapRender[Unit] =
    AliasMapRender { _ =>
      (aliasMap, ())
    }

}
