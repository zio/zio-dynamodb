package zio.dynamodb

import zio.test._

object MapOfSetSpec extends ZIOSpecDefault {

  def spec = suite("MapOfSet")(
    suite("+")(
      test("adds a value to an empty map") {
        val m = MapOfSet.empty[String, Int] + ("a" -> 1)
        assertTrue(m.get("a").contains(Set(1)))
      },
      test("adds to existing key — set union") {
        val m = (MapOfSet.empty[String, Int] + ("a" -> 1)) + ("a" -> 2)
        assertTrue(m.get("a").contains(Set(1, 2)))
      },
      test("adds to a different key") {
        val m = (MapOfSet.empty[String, Int] + ("a" -> 1)) + ("b" -> 2)
        assertTrue(m.get("a").contains(Set(1)) && m.get("b").contains(Set(2)))
      }
    ),
    suite("++ (MapOfSet)")(
      test("merges two disjoint maps") {
        val m1 = MapOfSet.empty[String, Int] + ("a" -> 1)
        val m2 = MapOfSet.empty[String, Int] + ("b" -> 2)
        val m  = m1 ++ m2
        assertTrue(m.get("a").contains(Set(1)) && m.get("b").contains(Set(2)))
      },
      test("merges overlapping keys — sets are unioned") {
        val m1 = MapOfSet.empty[String, Int] + ("a" -> 1)
        val m2 = MapOfSet.empty[String, Int] + ("a" -> 2)
        val m  = m1 ++ m2
        assertTrue(m.get("a").contains(Set(1, 2)))
      },
      test("merging with empty is identity") {
        val m1 = MapOfSet.empty[String, Int] + ("a" -> 1)
        val m  = m1 ++ MapOfSet.empty[String, Int]
        assertTrue(m.get("a").contains(Set(1)))
      }
    ),
    suite("++ (K, Iterable[V])")(
      test("adds all values for the given key") {
        val m = MapOfSet.empty[String, Int] ++ ("a" -> List(1, 2, 3))
        assertTrue(m.get("a").contains(Set(1, 2, 3)))
      },
      test("adds to existing key") {
        val m = (MapOfSet.empty[String, Int] + ("a" -> 1)) ++ ("a" -> List(2, 3))
        assertTrue(m.get("a").contains(Set(1, 2, 3)))
      }
    ),
    suite("addAll")(
      test("adds multiple key-value pairs") {
        val m = MapOfSet.empty[String, Int].addAll("a" -> 1, "b" -> 2, "a" -> 3)
        assertTrue(m.get("a").contains(Set(1, 3)) && m.get("b").contains(Set(2)))
      }
    ),
    suite("getOrElse")(
      test("returns set for existing key") {
        val m = MapOfSet.empty[String, Int] + ("a" -> 1)
        assertTrue(m.getOrElse("a", Set.empty) == Set(1))
      },
      test("returns default for missing key") {
        val m = MapOfSet.empty[String, Int]
        assertTrue(m.getOrElse("missing", Set(99)) == Set(99))
      }
    ),
    suite("toOption")(
      test("returns None for empty map") {
        assertTrue(MapOfSet.empty[String, Int].toOption.isEmpty)
      },
      test("returns Some for non-empty map") {
        val m = MapOfSet.empty[String, Int] + ("a" -> 1)
        assertTrue(m.toOption.isDefined)
      }
    ),
    suite("iterator")(
      test("iterates all key-set pairs") {
        val m    = (MapOfSet.empty[String, Int] + ("a" -> 1)) + ("b" -> 2)
        val keys = m.iterator.map(_._1).toSet
        assertTrue(keys == Set("a", "b"))
      }
    )
  )
}
