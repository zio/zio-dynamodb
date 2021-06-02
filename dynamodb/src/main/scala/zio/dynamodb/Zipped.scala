package zio.dynamodb

object Zipped {
  sealed trait Zippable[-A, -B] {
    type Out

    def zip(left: A, right: B): Out
  }
  object Zippable extends ZippableLowPriority {
    type Out[-A, -B, C] = Zippable[A, B] { type Out = C }

    implicit def Zippable3[A, B, Z]: Zippable.Out[(A, B), Z, (A, B, Z)] =
      new Zippable[(A, B), Z] {
        type Out = (A, B, Z)

        def zip(left: (A, B), right: Z): Out = (left._1, left._2, right)
      }

    implicit def Zippable4[A, B, C, Z]: Zippable.Out[(A, B, C), Z, (A, B, C, Z)] =
      new Zippable[(A, B, C), Z] {
        type Out = (A, B, C, Z)

        def zip(left: (A, B, C), right: Z): Out = (left._1, left._2, left._3, right)
      }

    implicit def Zippable5[A, B, C, D, Z]: Zippable.Out[(A, B, C, D), Z, (A, B, C, D, Z)] =
      new Zippable[(A, B, C, D), Z] {
        type Out = (A, B, C, D, Z)

        def zip(left: (A, B, C, D), right: Z): Out = (left._1, left._2, left._3, left._4, right)
      }
  }
  trait ZippableLowPriority {
    implicit def Zippable2[A, B]: Zippable.Out[A, B, (A, B)] =
      new Zippable[A, B] {
        type Out = (A, B)

        def zip(left: A, right: B): Out = (left, right)
      }
  }
}
