package zio.dynamodb

import zio.test.Assertion._
import zio.test._
import zio.test.ZIOSpecDefault

object MapOfSetSpec extends ZIOSpecDefault {

  override def spec =
    suite("MapOfSet")(
      test("addAll(1 -> 1, 1 -> 2)") {
        val actual   = MapOfSet.empty.addAll(1 -> 1, 1 -> 2)
        val expected = Set(1 -> Set(1, 2))
        assert(actual.toSet)(equalTo(expected))
      },
      test("addAll(1 -> 1, 1 -> 1)") {
        val actual   = MapOfSet.empty.addAll(1 -> 1, 1 -> 1)
        val expected = Set(1 -> Set(1, 1))
        assert(actual.toSet)(equalTo(expected))
      },
      test("+ (1 -> 1) and then + (1 -> 2)") {
        val actual   = (MapOfSet.empty + (1 -> 1)) + (1 -> 2)
        val expected = Set(1 -> Set(1, 2))
        assert(actual.toSet)(equalTo(expected))
      },
      test("+ (1 -> 1) and then + (1 -> 1)") {
        val actual   = (MapOfSet.empty + (1 -> 1)) + (1 -> 1)
        val expected = Set(1 -> Set(1))
        assert(actual.toSet)(equalTo(expected))
      },
      test("MapOfSet(1 -> 1) ++ MapOfSet(1 -> 2)") {
        val actual   = MapOfSet.empty.addAll(1 -> 1) ++ MapOfSet.empty.addAll(1 -> 2)
        val expected = Set(1 -> Set(1, 2))
        assert(actual.toSet)(equalTo(expected))
      },
      test("MapOfSet(1 -> 1) ++ MapOfSet(1 -> 1)") {
        val actual   = MapOfSet.empty.addAll(1 -> 1) ++ MapOfSet.empty.addAll(1 -> 1)
        val expected = Set(1 -> Set(1))
        assert(actual.toSet)(equalTo(expected))
      },
      test("get Some") {
        val actual   = MapOfSet.empty.addAll(1 -> 1, 1 -> 2)
        val expected = Some(Set(1, 2))
        assert(actual.get(1))(equalTo(expected))
      },
      test("get None") {
        val actual = MapOfSet.empty[Int, Int]
        assert(actual.get(1))(equalTo(None))
      },
      test("getOrElse found") {
        val actual = (MapOfSet.empty + (1 -> 1)) + (1 -> 2)
        assert(actual.getOrElse(1, Set(3)))(equalTo(Set(1, 2)))
      },
      test("getOrElse default") {
        val actual = (MapOfSet.empty + (1 -> 1)) + (1 -> 2)
        assert(actual.getOrElse(2, Set(3)))(equalTo(Set(3)))
      }
    )

}
