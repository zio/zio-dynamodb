package zio.dynamodb.examples

import zio.dynamodb.NonEmptySet

object MiscExamples extends App {

  val s = NonEmptySet("1") ++ NonEmptySet("2") + "3"
  println(s.toSet)
}
