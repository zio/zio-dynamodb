package zio.dynamodb

import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, ZSpec, _ }

object FromAttributeValueSpec extends DefaultRunnableSpec {

  override def spec: ZSpec[Environment, Failure] = suite("AttrMap suite")(fromAttributeValueSuite)

  val fromAttributeValueSuite: ZSpec[Environment, Failure] = suite("FromAttributeValueSuite")(
    test("get[String] should return a Right of String when it exists") {
      val attrMap = AttrMap("f1" -> "a")
      assert(attrMap.get[String]("f1"))(isRight(equalTo("a")))
    },
    test("get[String] should return a Left of String when it does not exists") {
      val attrMap = AttrMap.empty
      assert(attrMap.get[String]("f1"))(isLeft(equalTo("field 'f1' not found")))
    },
    test("getOpt[String] should return a Right of Some String when it exists") {
      val attrMap = AttrMap("f1" -> "a")
      assert(attrMap.getOpt[String]("f1"))(isRight(equalTo(Some("a"))))
    },
    test("getOpt[String] should return a Right of None when it does not exists") {
      val attrMap = AttrMap.empty
      assert(attrMap.getOpt[String]("f1"))(isRight(isNone))
    },
    test("getOpt[Item] should return a Right of Some Item when it exists") {
      val attrMap = AttrMap("f1" -> AttrMap("f2" -> "a"))
      assert(attrMap.getOpt[Item]("f1"))(isRight(isSome(equalTo(Item("f2" -> "a")))))
    },
    test("getOpt[Item] should return a Right of None when it does not exists") {
      val attrMap = AttrMap.empty
      assert(attrMap.getOpt[Item]("f1"))(isRight(isNone))
    },
    test("getOptItem should return a Right of Some(Foo) when it exists in the AttrMap") {
      final case class Foo(s: String, o: Option[String])
      val attrMap                             = AttrMap("f1" -> AttrMap("f2" -> "a", "f3" -> "b"))
      val either: Either[String, Option[Foo]] = for {
        maybe <- attrMap.getOptionalItem("f1") { m =>
                   for {
                     s <- m.get[String]("f2")
                     o <- m.getOpt[String]("f3")
                   } yield Foo(s, o)
                 }
      } yield maybe
      assert(either)(isRight(equalTo(Some(Foo("a", Some("b"))))))
    },
    test("getItemList should return a Right of Iterable[Foo] when it exists in the AttrMap") {
      final case class Foo(s: String, o: Option[String])
      val attrMap                               = AttrMap("f1" -> List(AttrMap("f2" -> "a", "f3" -> "b"), AttrMap("f2" -> "c", "f3" -> "d")))
      val either: Either[String, Iterable[Foo]] = for {
        xs <- attrMap.getIterableItem[Foo]("f1") { m =>
                for {
                  s <- m.get[String]("f2")
                  o <- m.getOpt[String]("f3")
                } yield Foo(s, o)
              }
      } yield xs
      assert(either)(isRight(equalTo(List(Foo("a", Some("b")), Foo("c", Some("d"))))))
    },
    test("getOptionalIterableItem should return a Right of Option[Iterable[Foo]] when it exists in the AttrMap") {
      final case class Foo(s: String, o: Option[String])
      val attrMap                                       = AttrMap("f1" -> List(AttrMap("f2" -> "a", "f3" -> "b"), AttrMap("f2" -> "c", "f3" -> "d")))
      val either: Either[String, Option[Iterable[Foo]]] = for {
        xs <- attrMap.getOptionalIterableItem[Foo]("f1") { m =>
                for {
                  s <- m.get[String]("f2")
                  o <- m.getOpt[String]("f3")
                } yield Foo(s, o)
              }
      } yield xs
      assert(either)(isRight(equalTo(Some(List(Foo("a", Some("b")), Foo("c", Some("d")))))))
    }
  )
}
