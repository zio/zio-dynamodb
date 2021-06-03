package zio.dynamodb

import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, _ }

object NonEmptySetSpec extends DefaultRunnableSpec {

  override def spec: ZSpec[Environment, Failure] =
    suite("NonEmptySet")(
      test("construction single param") {
        val actual = NonEmptySet(1)
        assert(actual.toSet)(equalTo(Set(1)))
      },
      test("construction multiple params") {
        val actual = NonEmptySet(1, 2)
        assert(actual.toSet)(equalTo(Set(1, 2)))
      },
      test("+") {
        val actual   = (NonEmptySet(1) + 2) + 3
        val expected = Set(1, 2, 3)
        assert(actual.toSet)(equalTo(expected))
      },
      test("++") {
        val set1   = (NonEmptySet(1) + 2) + 3
        val set2   = NonEmptySet(4, 5, 6, 7)
        val actual = set1 ++ set2
        assert(actual.toSet)(equalTo(Set(1, 2, 3, 4, 5, 6, 7)))
      }
    )

}
