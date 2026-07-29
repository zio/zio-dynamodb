package zio.dynamodb

private[dynamodb] trait GeneratedAttrMapApplies {

  def apply[A](t1: (String, A))(implicit a: ToAttributeValue[A]): AttrMap =
    AttrMap(JMapView.hash.single(t1._1, a.toAttributeValue(t1._2)))

  def apply[A, B](t1: (String, A), t2: (String, B))(implicit a: ToAttributeValue[A], b: ToAttributeValue[B]): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .result
    )

  def apply[A, B, C](t1: (String, A), t2: (String, B), t3: (String, C))(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .result
    )

  def apply[A, B, C, D](t1: (String, A), t2: (String, B), t3: (String, C), t4: (String, D))(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .result
    )

  def apply[A, B, C, D, E](t1: (String, A), t2: (String, B), t3: (String, C), t4: (String, D), t5: (String, E))(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .result
    )

  def apply[A, B, C, D, E, F](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .result
    )

  def apply[A, B, C, D, E, F, G](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .result
    )

  def apply[A, B, C, D, E, F, G, H](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G),
    t8: (String, H)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G],
    h: ToAttributeValue[H]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .addOne(t8._1, h.toAttributeValue(t8._2))
        .result
    )

  def apply[A, B, C, D, E, F, G, H, I](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G),
    t8: (String, H),
    t9: (String, I)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G],
    h: ToAttributeValue[H],
    i: ToAttributeValue[I]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .addOne(t8._1, h.toAttributeValue(t8._2))
        .addOne(t9._1, i.toAttributeValue(t9._2))
        .result
    )

  def apply[A, B, C, D, E, F, G, H, I, J](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G),
    t8: (String, H),
    t9: (String, I),
    t10: (String, J)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G],
    h: ToAttributeValue[H],
    i: ToAttributeValue[I],
    j: ToAttributeValue[J]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .addOne(t8._1, h.toAttributeValue(t8._2))
        .addOne(t9._1, i.toAttributeValue(t9._2))
        .addOne(t10._1, j.toAttributeValue(t10._2))
        .result
    )

  def apply[A, B, C, D, E, F, G, H, I, J, K](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G),
    t8: (String, H),
    t9: (String, I),
    t10: (String, J),
    t11: (String, K)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G],
    h: ToAttributeValue[H],
    i: ToAttributeValue[I],
    j: ToAttributeValue[J],
    k: ToAttributeValue[K]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .addOne(t8._1, h.toAttributeValue(t8._2))
        .addOne(t9._1, i.toAttributeValue(t9._2))
        .addOne(t10._1, j.toAttributeValue(t10._2))
        .addOne(t11._1, k.toAttributeValue(t11._2))
        .result
    )

  def apply[A, B, C, D, E, F, G, H, I, J, K, L](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G),
    t8: (String, H),
    t9: (String, I),
    t10: (String, J),
    t11: (String, K),
    t12: (String, L)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G],
    h: ToAttributeValue[H],
    i: ToAttributeValue[I],
    j: ToAttributeValue[J],
    k: ToAttributeValue[K],
    l: ToAttributeValue[L]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .addOne(t8._1, h.toAttributeValue(t8._2))
        .addOne(t9._1, i.toAttributeValue(t9._2))
        .addOne(t10._1, j.toAttributeValue(t10._2))
        .addOne(t11._1, k.toAttributeValue(t11._2))
        .addOne(t12._1, l.toAttributeValue(t12._2))
        .result
    )

  def apply[A, B, C, D, E, F, G, H, I, J, K, L, M](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G),
    t8: (String, H),
    t9: (String, I),
    t10: (String, J),
    t11: (String, K),
    t12: (String, L),
    t13: (String, M)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G],
    h: ToAttributeValue[H],
    i: ToAttributeValue[I],
    j: ToAttributeValue[J],
    k: ToAttributeValue[K],
    l: ToAttributeValue[L],
    m: ToAttributeValue[M]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .addOne(t8._1, h.toAttributeValue(t8._2))
        .addOne(t9._1, i.toAttributeValue(t9._2))
        .addOne(t10._1, j.toAttributeValue(t10._2))
        .addOne(t11._1, k.toAttributeValue(t11._2))
        .addOne(t12._1, l.toAttributeValue(t12._2))
        .addOne(t13._1, m.toAttributeValue(t13._2))
        .result
    )

  def apply[A, B, C, D, E, F, G, H, I, J, K, L, M, N](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G),
    t8: (String, H),
    t9: (String, I),
    t10: (String, J),
    t11: (String, K),
    t12: (String, L),
    t13: (String, M),
    t14: (String, N)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G],
    h: ToAttributeValue[H],
    i: ToAttributeValue[I],
    j: ToAttributeValue[J],
    k: ToAttributeValue[K],
    l: ToAttributeValue[L],
    m: ToAttributeValue[M],
    n: ToAttributeValue[N]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .addOne(t8._1, h.toAttributeValue(t8._2))
        .addOne(t9._1, i.toAttributeValue(t9._2))
        .addOne(t10._1, j.toAttributeValue(t10._2))
        .addOne(t11._1, k.toAttributeValue(t11._2))
        .addOne(t12._1, l.toAttributeValue(t12._2))
        .addOne(t13._1, m.toAttributeValue(t13._2))
        .addOne(t14._1, n.toAttributeValue(t14._2))
        .result
    )

  def apply[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G),
    t8: (String, H),
    t9: (String, I),
    t10: (String, J),
    t11: (String, K),
    t12: (String, L),
    t13: (String, M),
    t14: (String, N),
    t15: (String, O)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G],
    h: ToAttributeValue[H],
    i: ToAttributeValue[I],
    j: ToAttributeValue[J],
    k: ToAttributeValue[K],
    l: ToAttributeValue[L],
    m: ToAttributeValue[M],
    n: ToAttributeValue[N],
    o: ToAttributeValue[O]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .addOne(t8._1, h.toAttributeValue(t8._2))
        .addOne(t9._1, i.toAttributeValue(t9._2))
        .addOne(t10._1, j.toAttributeValue(t10._2))
        .addOne(t11._1, k.toAttributeValue(t11._2))
        .addOne(t12._1, l.toAttributeValue(t12._2))
        .addOne(t13._1, m.toAttributeValue(t13._2))
        .addOne(t14._1, n.toAttributeValue(t14._2))
        .addOne(t15._1, o.toAttributeValue(t15._2))
        .result
    )

  def apply[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G),
    t8: (String, H),
    t9: (String, I),
    t10: (String, J),
    t11: (String, K),
    t12: (String, L),
    t13: (String, M),
    t14: (String, N),
    t15: (String, O),
    t16: (String, P)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G],
    h: ToAttributeValue[H],
    i: ToAttributeValue[I],
    j: ToAttributeValue[J],
    k: ToAttributeValue[K],
    l: ToAttributeValue[L],
    m: ToAttributeValue[M],
    n: ToAttributeValue[N],
    o: ToAttributeValue[O],
    p: ToAttributeValue[P]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .addOne(t8._1, h.toAttributeValue(t8._2))
        .addOne(t9._1, i.toAttributeValue(t9._2))
        .addOne(t10._1, j.toAttributeValue(t10._2))
        .addOne(t11._1, k.toAttributeValue(t11._2))
        .addOne(t12._1, l.toAttributeValue(t12._2))
        .addOne(t13._1, m.toAttributeValue(t13._2))
        .addOne(t14._1, n.toAttributeValue(t14._2))
        .addOne(t15._1, o.toAttributeValue(t15._2))
        .addOne(t16._1, p.toAttributeValue(t16._2))
        .result
    )

  def apply[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G),
    t8: (String, H),
    t9: (String, I),
    t10: (String, J),
    t11: (String, K),
    t12: (String, L),
    t13: (String, M),
    t14: (String, N),
    t15: (String, O),
    t16: (String, P),
    t17: (String, Q)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G],
    h: ToAttributeValue[H],
    i: ToAttributeValue[I],
    j: ToAttributeValue[J],
    k: ToAttributeValue[K],
    l: ToAttributeValue[L],
    m: ToAttributeValue[M],
    n: ToAttributeValue[N],
    o: ToAttributeValue[O],
    p: ToAttributeValue[P],
    q: ToAttributeValue[Q]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .addOne(t8._1, h.toAttributeValue(t8._2))
        .addOne(t9._1, i.toAttributeValue(t9._2))
        .addOne(t10._1, j.toAttributeValue(t10._2))
        .addOne(t11._1, k.toAttributeValue(t11._2))
        .addOne(t12._1, l.toAttributeValue(t12._2))
        .addOne(t13._1, m.toAttributeValue(t13._2))
        .addOne(t14._1, n.toAttributeValue(t14._2))
        .addOne(t15._1, o.toAttributeValue(t15._2))
        .addOne(t16._1, p.toAttributeValue(t16._2))
        .addOne(t17._1, q.toAttributeValue(t17._2))
        .result
    )

  def apply[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G),
    t8: (String, H),
    t9: (String, I),
    t10: (String, J),
    t11: (String, K),
    t12: (String, L),
    t13: (String, M),
    t14: (String, N),
    t15: (String, O),
    t16: (String, P),
    t17: (String, Q),
    t18: (String, R)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G],
    h: ToAttributeValue[H],
    i: ToAttributeValue[I],
    j: ToAttributeValue[J],
    k: ToAttributeValue[K],
    l: ToAttributeValue[L],
    m: ToAttributeValue[M],
    n: ToAttributeValue[N],
    o: ToAttributeValue[O],
    p: ToAttributeValue[P],
    q: ToAttributeValue[Q],
    r: ToAttributeValue[R]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .addOne(t8._1, h.toAttributeValue(t8._2))
        .addOne(t9._1, i.toAttributeValue(t9._2))
        .addOne(t10._1, j.toAttributeValue(t10._2))
        .addOne(t11._1, k.toAttributeValue(t11._2))
        .addOne(t12._1, l.toAttributeValue(t12._2))
        .addOne(t13._1, m.toAttributeValue(t13._2))
        .addOne(t14._1, n.toAttributeValue(t14._2))
        .addOne(t15._1, o.toAttributeValue(t15._2))
        .addOne(t16._1, p.toAttributeValue(t16._2))
        .addOne(t17._1, q.toAttributeValue(t17._2))
        .addOne(t18._1, r.toAttributeValue(t18._2))
        .result
    )

  def apply[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G),
    t8: (String, H),
    t9: (String, I),
    t10: (String, J),
    t11: (String, K),
    t12: (String, L),
    t13: (String, M),
    t14: (String, N),
    t15: (String, O),
    t16: (String, P),
    t17: (String, Q),
    t18: (String, R),
    t19: (String, S)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G],
    h: ToAttributeValue[H],
    i: ToAttributeValue[I],
    j: ToAttributeValue[J],
    k: ToAttributeValue[K],
    l: ToAttributeValue[L],
    m: ToAttributeValue[M],
    n: ToAttributeValue[N],
    o: ToAttributeValue[O],
    p: ToAttributeValue[P],
    q: ToAttributeValue[Q],
    r: ToAttributeValue[R],
    s: ToAttributeValue[S]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .addOne(t8._1, h.toAttributeValue(t8._2))
        .addOne(t9._1, i.toAttributeValue(t9._2))
        .addOne(t10._1, j.toAttributeValue(t10._2))
        .addOne(t11._1, k.toAttributeValue(t11._2))
        .addOne(t12._1, l.toAttributeValue(t12._2))
        .addOne(t13._1, m.toAttributeValue(t13._2))
        .addOne(t14._1, n.toAttributeValue(t14._2))
        .addOne(t15._1, o.toAttributeValue(t15._2))
        .addOne(t16._1, p.toAttributeValue(t16._2))
        .addOne(t17._1, q.toAttributeValue(t17._2))
        .addOne(t18._1, r.toAttributeValue(t18._2))
        .addOne(t19._1, s.toAttributeValue(t19._2))
        .result
    )

  def apply[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G),
    t8: (String, H),
    t9: (String, I),
    t10: (String, J),
    t11: (String, K),
    t12: (String, L),
    t13: (String, M),
    t14: (String, N),
    t15: (String, O),
    t16: (String, P),
    t17: (String, Q),
    t18: (String, R),
    t19: (String, S),
    t20: (String, T)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G],
    h: ToAttributeValue[H],
    i: ToAttributeValue[I],
    j: ToAttributeValue[J],
    k: ToAttributeValue[K],
    l: ToAttributeValue[L],
    m: ToAttributeValue[M],
    n: ToAttributeValue[N],
    o: ToAttributeValue[O],
    p: ToAttributeValue[P],
    q: ToAttributeValue[Q],
    r: ToAttributeValue[R],
    s: ToAttributeValue[S],
    t: ToAttributeValue[T]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .addOne(t8._1, h.toAttributeValue(t8._2))
        .addOne(t9._1, i.toAttributeValue(t9._2))
        .addOne(t10._1, j.toAttributeValue(t10._2))
        .addOne(t11._1, k.toAttributeValue(t11._2))
        .addOne(t12._1, l.toAttributeValue(t12._2))
        .addOne(t13._1, m.toAttributeValue(t13._2))
        .addOne(t14._1, n.toAttributeValue(t14._2))
        .addOne(t15._1, o.toAttributeValue(t15._2))
        .addOne(t16._1, p.toAttributeValue(t16._2))
        .addOne(t17._1, q.toAttributeValue(t17._2))
        .addOne(t18._1, r.toAttributeValue(t18._2))
        .addOne(t19._1, s.toAttributeValue(t19._2))
        .addOne(t20._1, t.toAttributeValue(t20._2))
        .result
    )

  def apply[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G),
    t8: (String, H),
    t9: (String, I),
    t10: (String, J),
    t11: (String, K),
    t12: (String, L),
    t13: (String, M),
    t14: (String, N),
    t15: (String, O),
    t16: (String, P),
    t17: (String, Q),
    t18: (String, R),
    t19: (String, S),
    t20: (String, T),
    t21: (String, U)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G],
    h: ToAttributeValue[H],
    i: ToAttributeValue[I],
    j: ToAttributeValue[J],
    k: ToAttributeValue[K],
    l: ToAttributeValue[L],
    m: ToAttributeValue[M],
    n: ToAttributeValue[N],
    o: ToAttributeValue[O],
    p: ToAttributeValue[P],
    q: ToAttributeValue[Q],
    r: ToAttributeValue[R],
    s: ToAttributeValue[S],
    t: ToAttributeValue[T],
    u: ToAttributeValue[U]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .addOne(t8._1, h.toAttributeValue(t8._2))
        .addOne(t9._1, i.toAttributeValue(t9._2))
        .addOne(t10._1, j.toAttributeValue(t10._2))
        .addOne(t11._1, k.toAttributeValue(t11._2))
        .addOne(t12._1, l.toAttributeValue(t12._2))
        .addOne(t13._1, m.toAttributeValue(t13._2))
        .addOne(t14._1, n.toAttributeValue(t14._2))
        .addOne(t15._1, o.toAttributeValue(t15._2))
        .addOne(t16._1, p.toAttributeValue(t16._2))
        .addOne(t17._1, q.toAttributeValue(t17._2))
        .addOne(t18._1, r.toAttributeValue(t18._2))
        .addOne(t19._1, s.toAttributeValue(t19._2))
        .addOne(t20._1, t.toAttributeValue(t20._2))
        .addOne(t21._1, u.toAttributeValue(t21._2))
        .result
    )

  def apply[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](
    t1: (String, A),
    t2: (String, B),
    t3: (String, C),
    t4: (String, D),
    t5: (String, E),
    t6: (String, F),
    t7: (String, G),
    t8: (String, H),
    t9: (String, I),
    t10: (String, J),
    t11: (String, K),
    t12: (String, L),
    t13: (String, M),
    t14: (String, N),
    t15: (String, O),
    t16: (String, P),
    t17: (String, Q),
    t18: (String, R),
    t19: (String, S),
    t20: (String, T),
    t21: (String, U),
    t22: (String, V)
  )(implicit
    a: ToAttributeValue[A],
    b: ToAttributeValue[B],
    c: ToAttributeValue[C],
    d: ToAttributeValue[D],
    e: ToAttributeValue[E],
    f: ToAttributeValue[F],
    g: ToAttributeValue[G],
    h: ToAttributeValue[H],
    i: ToAttributeValue[I],
    j: ToAttributeValue[J],
    k: ToAttributeValue[K],
    l: ToAttributeValue[L],
    m: ToAttributeValue[M],
    n: ToAttributeValue[N],
    o: ToAttributeValue[O],
    p: ToAttributeValue[P],
    q: ToAttributeValue[Q],
    r: ToAttributeValue[R],
    s: ToAttributeValue[S],
    t: ToAttributeValue[T],
    u: ToAttributeValue[U],
    v: ToAttributeValue[V]
  ): AttrMap =
    AttrMap(
      JMapView.hash
        .builder[String, AttributeValue]
        .addOne(t1._1, a.toAttributeValue(t1._2))
        .addOne(t2._1, b.toAttributeValue(t2._2))
        .addOne(t3._1, c.toAttributeValue(t3._2))
        .addOne(t4._1, d.toAttributeValue(t4._2))
        .addOne(t5._1, e.toAttributeValue(t5._2))
        .addOne(t6._1, f.toAttributeValue(t6._2))
        .addOne(t7._1, g.toAttributeValue(t7._2))
        .addOne(t8._1, h.toAttributeValue(t8._2))
        .addOne(t9._1, i.toAttributeValue(t9._2))
        .addOne(t10._1, j.toAttributeValue(t10._2))
        .addOne(t11._1, k.toAttributeValue(t11._2))
        .addOne(t12._1, l.toAttributeValue(t12._2))
        .addOne(t13._1, m.toAttributeValue(t13._2))
        .addOne(t14._1, n.toAttributeValue(t14._2))
        .addOne(t15._1, o.toAttributeValue(t15._2))
        .addOne(t16._1, p.toAttributeValue(t16._2))
        .addOne(t17._1, q.toAttributeValue(t17._2))
        .addOne(t18._1, r.toAttributeValue(t18._2))
        .addOne(t19._1, s.toAttributeValue(t19._2))
        .addOne(t20._1, t.toAttributeValue(t20._2))
        .addOne(t21._1, u.toAttributeValue(t21._2))
        .addOne(t22._1, v.toAttributeValue(t22._2))
        .result
    )
}
