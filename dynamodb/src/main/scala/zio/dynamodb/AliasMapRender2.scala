package zio.dynamodb

private[dynamodb] trait Renderable2 {
  def render2: AliasMapRender2[String]
}

private[dynamodb] final case class AliasMapRender2[+A](
  render: AliasMap2 => (AliasMap2, A)
) { self =>

  def map[B](f: A => B): AliasMapRender2[B] =
    AliasMapRender2 { aliasMap =>
      val (am, a) = self.render(aliasMap)
      (am, f(a))
    }

  def flatMap[B](f: A => AliasMapRender2[B]): AliasMapRender2[B] =
    AliasMapRender2 { aliasMap =>
      val (am, a) = self.render(aliasMap)
      f(a).render(am)
    }

  def zipWith[B, C](that: AliasMapRender2[B])(f: (A, B) => C): AliasMapRender2[C] =
    for {
      a <- self
      b <- that
    } yield f(a, b)

  def zipRight[B](that: AliasMapRender2[B]): AliasMapRender2[B] =
    self.flatMap(_ => that)

  def execute: (AliasMap2, A) = self.render(AliasMap2.empty)

}

private[dynamodb] object AliasMapRender2 {
  def getOrInsert(entry: AttributeValue): AliasMapRender2[String] =
    AliasMapRender2 { aliasMap =>
      aliasMap.getOrInsert(entry)
    }
  def getOrInsert[From, To](entry: ProjectionExpression[From, To]): AliasMapRender2[String] =
    AliasMapRender2 { aliasMap =>
      aliasMap.getOrInsert(entry)
    }

  def empty: AliasMapRender2[Unit] = AliasMapRender2.addMap(AliasMap2.empty)

  def succeed[A](a: => A): AliasMapRender2[A] = AliasMapRender2(aliasMap => (aliasMap, a))

  val getMap: AliasMapRender2[AliasMap2] = AliasMapRender2(aliasMap => (aliasMap, aliasMap))

  def addMap(aliasMap: AliasMap2): AliasMapRender2[Unit] =
    AliasMapRender2 { oldMap =>
      (oldMap ++ aliasMap, ())
    }

  def collectAll[A](optional: Option[AliasMapRender2[A]]): AliasMapRender2[Option[A]] =
    optional match {
      case Some(value) => value.map(Some(_))
      case None        => AliasMapRender2.succeed(None)
    }

}
