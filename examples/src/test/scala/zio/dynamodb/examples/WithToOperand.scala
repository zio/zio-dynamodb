//package zio.dynamodb.examples
//
//object WithToOperand extends App {
//  trait ToOperand[A] {}
//
//  implicit val _1: ToOperand[String] = new ToOperand[String] {}
//
//  final case class ProjectionExpression[A](value: String)
//
//  object ProjectionExpression extends LowPriorityImplicits {
//    implicit class ToInt1[A: ToOperand](option: ProjectionExpression[A]) { // context bound of ToOperand
//      def ===(that: A): Boolean = true // (1)
//    }
//  }
//  trait LowPriorityImplicits {
//    implicit class ToInt2[A](option: ProjectionExpression[A]) {
//      def ===(that: ProjectionExpression[A]): Boolean = false // (2)
//    }
//  } // (1) and (2) are extension methods that override ===
//
//  println(ProjectionExpression[String]("bar") === "bar")
//  println(ProjectionExpression[Int]("foo") === ProjectionExpression[Int]("foo"))
//}
