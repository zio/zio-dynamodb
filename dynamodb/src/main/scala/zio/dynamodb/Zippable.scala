package zio.dynamodb

sealed trait Zippable[-A, -B] {
  type Out

  def zip(left: A, right: B): Out
}
object Zippable extends ZippableLowPriority1 {
  type Out[-A, -B, C] = Zippable[A, B] { type Out = C }

  private[dynamodb] implicit def ZippableUnit[A]: Zippable.Out[A, Unit, A] =
    new Zippable[A, Unit] {
      type Out = A

      def zip(left: A, right: Unit): Out = left
    }

}

trait ZippableLowPriority1 extends ZippableLowPriority2 {
  private[dynamodb] implicit def Zippable3[A, B, Z]: Zippable.Out[(A, B), Z, (A, B, Z)] =
    new Zippable[(A, B), Z] {
      type Out = (A, B, Z)

      def zip(left: (A, B), right: Z): Out = (left._1, left._2, right)
    }

  private[dynamodb] implicit def Zippable4[A, B, C, Z]: Zippable.Out[(A, B, C), Z, (A, B, C, Z)] =
    new Zippable[(A, B, C), Z] {
      type Out = (A, B, C, Z)

      def zip(left: (A, B, C), right: Z): Out = (left._1, left._2, left._3, right)
    }

}
trait ZippableLowPriority2 extends ZippableLowPriority3 {

  implicit def Zippable2Right[B]: Zippable.Out[Unit, B, B] =
    new Zippable[Unit, B] {
      type Out = B

      def zip(left: Unit, right: B): Out = right
    }
}
trait ZippableLowPriority3 extends ZippableLowPriority4 {
  implicit def Zippable2Left[A]: Zippable.Out[A, Unit, A] =
    new Zippable[A, Unit] {
      type Out = A

      def zip(left: A, right: Unit): Out = left
    }

}

trait ZippableLowPriority4 {
  implicit def Zippable2[A, B]: Zippable.Out[A, B, (A, B)] =
    new Zippable[A, B] {
      type Out = (A, B)

      def zip(left: A, right: B): Out = (left, right)
    }
}
