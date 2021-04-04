package zio.dynamodb

import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, _ }

//noinspection TypeAnnotation
object NonSetEmptySpec extends DefaultRunnableSpec {

  override def spec =
    suite("NonEmptySet")(
      test("perform construction and + and ++") {
        val set1 = NonEmptySet(1)
        val set3 = (NonEmptySet(1) + 2) + 3
        val set4 = NonEmptySet(4, 5, 6, 7)
        val set7 = set3 ++ set4

        assert(set1.toSet)(equalTo(Set(1))) && assert(set3.toSet)(equalTo(Set(1, 2, 3))) && assert(set4.toSet)(
          equalTo(Set(4, 5, 6, 7))
        ) && assert(set7.toSet)(equalTo(Set(1, 2, 3, 4, 5, 6, 7)))
      }
    )

}
