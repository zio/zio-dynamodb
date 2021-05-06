package zio.dynamodb

import zio.dynamodb.ProjectionExpression.{ ListElement, MapElement, Root }
import zio.test.Assertion.{ equalTo, isLeft, isRight }
import zio.test.{ DefaultRunnableSpec, _ }

object ProjectionExpressionParserSpec extends DefaultRunnableSpec {
  override def spec =
    suite("ProjectionExpression Parser")(
      test(""" "foo" returns Root("foo") """) {
        val actual = ProjectionExpression.parse("foo")
        assert(actual)(isRight(equalTo(Root("foo"))))
      },
      test(""" "foo.bar" returns Right(MapElement(Root("foo"),"bar")) """) {
        val actual = ProjectionExpression.parse("foo.bar")
        assert(actual)(isRight(equalTo(MapElement(Root("foo"), "bar"))))
      },
      test(""" "foo.bar[9]" returns Right(ListElement(MapElement(Root("foo"), "bar")) """) {
        val actual = ProjectionExpression.parse("foo.bar[9]")
        assert(actual)(isRight(equalTo(ListElement(MapElement(Root("foo"), "bar"), 9))))
      },
      test(""" "foo.bar[9].baz" returns Right(MapElement(ListElement(MapElement(Root("foo"),"bar"),9),"baz")) """) {
        val actual = ProjectionExpression.parse("foo.bar[9].baz")
        assert(actual)(isRight(equalTo(MapElement(ListElement(MapElement(Root("foo"), "bar"), 9), "baz"))))
      },
      test("returns error for empty string") {
        val actual = ProjectionExpression.parse("")
        assert(actual)(isLeft(equalTo("error with ''")))
      },
      test("returns error for for '.'") {
        val actual = ProjectionExpression.parse(".")
        assert(actual)(isLeft(equalTo("error - at least one element must be specified")))
      },
      test("returns multiple errors - one for each syntax violation") {
        val actual = ProjectionExpression.parse("fo$o.ba$r[9].ba$z")
        assert(actual)(isLeft(equalTo("error with 'fo$o',error with 'ba$r[9]',error with 'ba$z'")))
      },
      test(""" "foo[X]" returns Left("error with 'foo[X]'")""") {
        val actual = ProjectionExpression.parse("foo[X]")
        assert(actual)(isLeft(equalTo("error with 'foo[X]'")))
      }
    )
}
