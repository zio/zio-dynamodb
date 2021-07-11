package zio.dynamodb

import zio.test.Assertion.{ equalTo, isLeft, isNone, isRight, isSome }
import zio.test.{ DefaultRunnableSpec, ZSpec, _ }

object FromAttributeValueSpec extends DefaultRunnableSpec {

  override def spec: ZSpec[Environment, Failure] = suite("AttrMap suite")(fromAttributeValueSuite)

  val fromAttributeValueSuite = suite("FromAttributeValueSuite")(
    test("get[String] should return a Right of String when it exists") {
      val attrMap = AttrMap("f1" -> "a")
      assert(attrMap.get[String]("f1"))(isRight(equalTo("a")))
    },
    test("get[String] should return a Left of String when it does not exists") {
      val attrMap = AttrMap("f2" -> "a")
      assert(attrMap.get[String]("f1"))(isLeft(equalTo("field 'f1' not found")))
    },
    test("getOpt[String] should return a Right of Some String when it exists") {
      val attrMap = AttrMap("f1" -> "a")
      assert(attrMap.getOpt[String]("f1"))(isSome(equalTo("a")))
    },
    test("getOpt[String] should return a Right of None when it does not exists") {
      val attrMap = AttrMap("f2" -> "a")
      assert(attrMap.getOpt[String]("f1"))(isNone)
    },
    test("getOpt[Item] should return a Right of Some Item when it exists") {
      val attrMap = AttrMap("f1" -> AttrMap("f2" -> "a"))
      assert(attrMap.getOpt[Item]("f1"))(isSome(equalTo(Item("f2" -> "a"))))
    },
    test("getOpt[Item] should return a Right of None when it does not exists") {
      val attrMap = AttrMap("f2" -> AttrMap("f2" -> "a"))
      assert(attrMap.getOpt[Item]("f1"))(isNone)
    },
    test("getOpt[Item] should return a Right of Some(Foo) when it exists") {
      final case class Foo(s: String)
      val attrMap                             = AttrMap("f1" -> AttrMap("f2" -> "a"))
      val either: Either[String, Option[Foo]] = for {
        maybe <- attrMap
                   .getOpt[Item]("f1")
                   .fold[Either[String, Option[Foo]]](Right(None)) { m =>
                     for {
                       s <- m.get[String]("f2")
                     } yield Some(Foo(s))
                   }
      } yield maybe
      assert(either)(isRight(equalTo(Some(Foo("a")))))
    },
    test("getOptItem should return a Right of Some(Foo) when it exists") {
      final case class Foo(s: String)
      val attrMap                             = AttrMap("f1" -> AttrMap("f2" -> "a"))
      val either: Either[String, Option[Foo]] = for {
        maybe <- attrMap.getOptItem("f1") { m =>
                   for {
                     s <- m.get[String]("f2")
                   } yield Some(Foo(s))
                 }
      } yield maybe
      assert(either)(isRight(equalTo(Some(Foo("a")))))
    }
  )
}
