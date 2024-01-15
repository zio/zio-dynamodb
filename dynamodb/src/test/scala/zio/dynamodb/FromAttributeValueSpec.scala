package zio.dynamodb

import zio.dynamodb.DynamoDBError.DecodingError
import zio.test.Assertion._
import zio.test.{ ZIOSpecDefault, _ }

object FromAttributeValueSpec extends ZIOSpecDefault {

  override def spec: Spec[Any, Nothing] = suite("AttrMap suite")(fromAttributeValueSuite)

  val fromAttributeValueSuite = suite("FromAttributeValueSuite")(
    test("get[String] should return a Right of String when it exists") {
      val attrMap = AttrMap("f1" -> "a")
      assert(attrMap.get[String]("f1"))(isRight(equalTo("a")))
    },
    test("get[String] should return a Left of String when it does not exists") {
      val attrMap = AttrMap.empty
      assert(attrMap.get[String]("f1"))(isLeft(equalTo(DecodingError("field 'f1' not found"))))
    },
    test("getOptional[String] should return a Right of Some String when it exists") {
      val attrMap = AttrMap("f1" -> "a")
      assert(attrMap.getOptional[String]("f1"))(isRight(equalTo(Some("a"))))
    },
    test("getOptional[String] should return a Right of None when it does not exists") {
      val attrMap = AttrMap.empty
      assert(attrMap.getOptional[String]("f1"))(isRight(isNone))
    },
    test("getOptional[Item] should return a Right of Some Item when it exists") {
      val attrMap = AttrMap("f1" -> AttrMap("f2" -> "a"))
      assert(attrMap.getOptional[Item]("f1"))(isRight(isSome(equalTo(Item("f2" -> "a")))))
    },
    test("getOptional[Item] should return a Right of None when it does not exists") {
      val attrMap = AttrMap.empty
      assert(attrMap.getOptional[Item]("f1"))(isRight(isNone))
    },
    test("getOptItem should return a Right of Some(Foo) when it exists in the AttrMap") {
      final case class Foo(s: String, o: Option[String])
      val attrMap                                    = AttrMap("f1" -> AttrMap("f2" -> "a", "f3" -> "b"))
      val either: Either[DynamoDBItemError, Option[Foo]] = for {
        maybe <- attrMap.getOptionalItem("f1") { m =>
                   for {
                     s <- m.get[String]("f2")
                     o <- m.getOptional[String]("f3")
                   } yield Foo(s, o)
                 }
      } yield maybe
      assert(either)(isRight(equalTo(Some(Foo("a", Some("b"))))))
    },
    test("getItemList should return a Right of Iterable[Foo] when it exists in the AttrMap") {
      final case class Foo(s: String, o: Option[String])
      val attrMap                                      = AttrMap("f1" -> List(AttrMap("f2" -> "a", "f3" -> "b"), AttrMap("f2" -> "c", "f3" -> "d")))
      val either: Either[DynamoDBItemError, Iterable[Foo]] = for {
        xs <- attrMap.getIterableItem[Foo]("f1") { m =>
                for {
                  s <- m.get[String]("f2")
                  o <- m.getOptional[String]("f3")
                } yield Foo(s, o)
              }
      } yield xs
      assert(either)(isRight(equalTo(List(Foo("a", Some("b")), Foo("c", Some("d"))))))
    },
    test("getOptionalIterableItem should return a Right of Option[Iterable[Foo]] when it exists in the AttrMap") {
      final case class Foo(s: String, o: Option[String])
      val attrMap                                              = AttrMap("f1" -> List(AttrMap("f2" -> "a", "f3" -> "b"), AttrMap("f2" -> "c", "f3" -> "d")))
      val either: Either[DynamoDBItemError, Option[Iterable[Foo]]] = for {
        xs <- attrMap.getOptionalIterableItem[Foo]("f1") { m =>
                for {
                  s <- m.get[String]("f2")
                  o <- m.getOptional[String]("f3")
                } yield Foo(s, o)
              }
      } yield xs
      assert(either)(isRight(equalTo(Some(List(Foo("a", Some("b")), Foo("c", Some("d")))))))
    },
    test("as with an optional field that is present") {
      final case class Person(address: Address)
      final case class Address(line1: String, line2: Option[String])
      val item   = Item("address" -> Item("line1" -> "line1", "line2" -> "line2"))
      val actual = for {
        address <- item.getItem("address")(m => m.as("line1", "line2")(Address.apply))
      } yield Person(address)
      assert(actual)(isRight(equalTo(Person(Address("line1", Some("line2"))))))
    },
    test("as with an optional field that is NOT present") {
      final case class Person(address: Address)
      final case class Address(line1: String, line2: Option[String])
      val item   = Item("address" -> Item("line1" -> "line1", "line2" -> null))
      val actual = for {
        address <- item.getItem("address")(m => m.as("line1", "line2")(Address.apply))
      } yield Person(address)
      assert(actual)(isRight(equalTo(Person(Address("line1", None)))))
    }
  )

}
