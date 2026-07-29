package zio.dynamodb

import zio.test._

object ZippableSpec extends ZIOSpecDefault {

  def spec = suite("Zippable")(
    test("Zippable5 zips (A,B,C,D) with Z into (A,B,C,D,Z)") {
      val z = implicitly[Zippable.Out[(Int, String, Boolean, Double), Long, (Int, String, Boolean, Double, Long)]]
      assertTrue(z.zip((1, "a", true, 2.0), 3L) == ((1, "a", true, 2.0, 3L)))
    },
    test("Zippable6 zips (A,B,C,D,E) with Z into (A,B,C,D,E,Z)") {
      val z = implicitly[
        Zippable.Out[(Int, String, Boolean, Double, Long), Float, (Int, String, Boolean, Double, Long, Float)]
      ]
      assertTrue(z.zip((1, "a", true, 2.0, 3L), 4.0f) == ((1, "a", true, 2.0, 3L, 4.0f)))
    },
    test("Zippable7 zips (A,B,C,D,E,F) with Z") {
      val z = implicitly[Zippable.Out[
        (Int, String, Boolean, Double, Long, Float),
        Char,
        (Int, String, Boolean, Double, Long, Float, Char)
      ]]
      assertTrue(z.zip((1, "a", true, 2.0, 3L, 4.0f), 'x') == ((1, "a", true, 2.0, 3L, 4.0f, 'x')))
    },
    test("Zippable8 zips (A,B,C,D,E,F,G) with Z") {
      val z = implicitly[Zippable.Out[
        (Int, String, Boolean, Double, Long, Float, Char),
        Short,
        (Int, String, Boolean, Double, Long, Float, Char, Short)
      ]]
      assertTrue(z.zip((1, "a", true, 2.0, 3L, 4.0f, 'x'), 5.toShort)._8 == 5.toShort)
    },
    test("Zippable9 zips 8-tuple with element") {
      val z      = implicitly[
        Zippable.Out[(Int, Int, Int, Int, Int, Int, Int, Int), Int, (Int, Int, Int, Int, Int, Int, Int, Int, Int)]
      ]
      val result = z.zip((1, 2, 3, 4, 5, 6, 7, 8), 9)
      assertTrue(result._9 == 9)
    },
    test("Zippable10 zips 9-tuple with element") {
      val z      = implicitly[Zippable.Out[
        (Int, Int, Int, Int, Int, Int, Int, Int, Int),
        Int,
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
      ]]
      val result = z.zip((1, 2, 3, 4, 5, 6, 7, 8, 9), 10)
      assertTrue(result._10 == 10)
    },
    test("Zippable11 zips 10-tuple with element") {
      val z      = implicitly[Zippable.Out[
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int),
        Int,
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
      ]]
      val result = z.zip((1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 11)
      assertTrue(result._11 == 11)
    },
    test("Zippable12 zips 11-tuple with element") {
      val z      = implicitly[Zippable.Out[
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int),
        Int,
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
      ]]
      val result = z.zip((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), 12)
      assertTrue(result._12 == 12)
    },
    test("Zippable13 zips 12-tuple with element") {
      val z      = implicitly[Zippable.Out[
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int),
        Int,
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
      ]]
      val result = z.zip((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), 13)
      assertTrue(result._13 == 13)
    },
    test("Zippable14 zips 13-tuple with element") {
      val z      = implicitly[Zippable.Out[
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int),
        Int,
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
      ]]
      val result = z.zip((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13), 14)
      assertTrue(result._14 == 14)
    },
    test("Zippable15 zips 14-tuple with element") {
      val z      = implicitly[Zippable.Out[
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int),
        Int,
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
      ]]
      val result = z.zip((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14), 15)
      assertTrue(result._15 == 15)
    },
    test("Zippable16 zips 15-tuple with element") {
      val z      = implicitly[Zippable.Out[
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int),
        Int,
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
      ]]
      val result = z.zip((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15), 16)
      assertTrue(result._16 == 16)
    },
    test("Zippable17 zips 16-tuple with element") {
      val z      = implicitly[Zippable.Out[
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int),
        Int,
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
      ]]
      val result = z.zip((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16), 17)
      assertTrue(result._17 == 17)
    },
    test("Zippable18 zips 17-tuple with element") {
      val z      = implicitly[Zippable.Out[
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int),
        Int,
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
      ]]
      val result = z.zip((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17), 18)
      assertTrue(result._18 == 18)
    },
    test("Zippable19 zips 18-tuple with element") {
      val z      = implicitly[Zippable.Out[
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int),
        Int,
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
      ]]
      val result = z.zip((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18), 19)
      assertTrue(result._19 == 19)
    },
    test("Zippable20 zips 19-tuple with element") {
      val z      = implicitly[Zippable.Out[
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int),
        Int,
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
      ]]
      val result = z.zip((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19), 20)
      assertTrue(result._20 == 20)
    },
    test("Zippable21 zips 20-tuple with element") {
      val z      = implicitly[Zippable.Out[
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int),
        Int,
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
      ]]
      val result = z.zip((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20), 21)
      assertTrue(result._21 == 21)
    },
    test("Zippable22 zips 21-tuple with element") {
      val z      = implicitly[Zippable.Out[
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int),
        Int,
        (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
      ]]
      val result = z.zip((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21), 22)
      assertTrue(result._22 == 22)
    },
    test("Zippable2Right zips Unit and B into B") {
      val z = implicitly[Zippable.Out[Unit, String, String]]
      assertTrue(z.zip((), "hello") == "hello")
    },
    test("Zippable2Left zips A and Unit into A") {
      val z = implicitly[Zippable.Out[String, Unit, String]]
      assertTrue(z.zip("hello", ()) == "hello")
    },
    test("Zippable2 (default) zips two values into a tuple") {
      val result = DynamoDBQuery(1) zipPar DynamoDBQuery("a")
      val (a, b) = DummyIOInterpreter.run(result).unsafeRun()
      assertTrue(a == 1 && b == "a")
    }
  )
}
