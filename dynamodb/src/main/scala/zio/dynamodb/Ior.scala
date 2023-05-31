package zio.dynamodb

sealed trait Ior[+A, +B]

case class LeftIor[+A, +B](value: A) extends Ior[A, B]

case class RightIor[+A, +B](value: B) extends Ior[A, B]

case class BothIor[+A, +B](valueA: A, valueB: B) extends Ior[A, B]
