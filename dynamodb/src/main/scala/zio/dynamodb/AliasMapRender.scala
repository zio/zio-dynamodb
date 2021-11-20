package zio.dynamodb

final case class AliasMapRender[+A](
  render: AliasMap => (AliasMap, A)
) { self =>

  def map[B](f: A => B): AliasMapRender[B] =
    AliasMapRender { aliasMap =>
      val (am, a) = self.render(aliasMap)
      (am, f(a))
    }

  def flatMap[B](f: A => AliasMapRender[B]): AliasMapRender[B] =
    AliasMapRender { aliasMap =>
      val (am, a) = self.render(aliasMap)
      f(a).render(am)
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
