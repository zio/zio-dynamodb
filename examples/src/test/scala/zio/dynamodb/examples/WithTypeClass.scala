//package zio.dynamodb.examples
//
//object WithTypeClass extends App {
//  trait TypeClass[A] {}
//
//  implicit val _1: TypeClass[Int] = new TypeClass[Int] {}
//
//  implicit class ToInt1[A: TypeClass](option: Option[A]) {
//    def +(that: Int): Int = option.get.asInstanceOf[Int] + that
//  }
//  implicit class ToInt2[A](option: Option[A])            {
//    def +(that: Option[Int]): Int = option.get.hashCode + that.get
//  }
//
//  println(Some(123) + 3)
//  println(Some("foo") + Some(3))
//}
