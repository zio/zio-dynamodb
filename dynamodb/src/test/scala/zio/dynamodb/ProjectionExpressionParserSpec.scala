package zio.dynamodb

import zio.dynamodb.ProjectionExpression.{ parse, ListElement, MapElement, Root }
import zio.random.Random
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, _ }

object ProjectionExpressionParserSpec extends DefaultRunnableSpec {
  object Generators {
    def fieldName                                                                                    = Gen.anyString
    def index                                                                                        = Gen.anyInt
    def root: Gen[Random with Sized, Root]                                                           = fieldName.map(Root)
    def mapElement(parent: => ProjectionExpression)                                                  = fieldName.map(MapElement(parent, _))
    def listElement(parent: => ProjectionExpression)                                                 = index.map(ListElement(parent, _))
    def mapOrListElement(parent: ProjectionExpression): Gen[Random with Sized, ProjectionExpression] =
      Gen.oneOf(mapElement(parent))

    def projectionExpression: Gen[Random with Sized, ProjectionExpression] = {
      def loop(
        parentGen: Gen[Random with Sized, ProjectionExpression],
        counter: Int
      ): Gen[Random with Sized, ProjectionExpression] =
        if (counter == 0)
          parentGen
        else
          loop(parentGen.flatMap(pe => mapOrListElement(pe)), counter - 1)

      for {
        count <- Gen.int(1, 20)
        pe    <- loop(root, count)
      } yield pe
    }

  }

  override def spec: ZSpec[Environment, Failure] =
    suite("ProjectionExpression Parser")(
      // TODO
      testM("should report syntax errors with non empty invalid ProjectionExpressions") {
        check(Generators.projectionExpression) { pe =>
          assert(
            parse(pe.toString)
          ) {
            isLeft(
              startsWithString(
                "error with"
              )
            )
          }
        }
      },
      test("toString on a ProjectionExpression of foo.bar[9].baz") {
        val pe = MapElement(ListElement(MapElement(Root("foo"), "bar"), 9), "baz")
        assert(pe.toString)(equalTo("foo.bar[9].baz"))
      },
      test("""'foo' returns Root("foo") """) {
        val actual   = parse("foo")
        val expected = Root("foo")
        assert(actual)(isRight(equalTo(expected)))
      },
      test("""'foo.bar' returns Right(MapElement(Root("foo"),"bar")) """) {
        val actual   = parse("foo.bar")
        val expected = MapElement(Root("foo"), "bar")
        assert(actual)(isRight(equalTo(expected)))
      },
      test("""'foo.bar[9]' returns Right(ListElement(MapElement(Root("foo"), "bar"), 9)) """) {
        val actual   = parse("foo.bar[9]")
        val expected = ListElement(MapElement(Root("foo"), "bar"), 9)
        assert(actual)(isRight(equalTo(expected)))
      },
      test("""'foo.bar[9].baz' returns Right(MapElement(ListElement(MapElement(Root("foo"),"bar"),9),"baz")) """) {
        val actual   = parse("foo.bar[9].baz")
        val expected = MapElement(ListElement(MapElement(Root("foo"), "bar"), 9), "baz")
        assert(actual)(isRight(equalTo(expected)))
      },
      test("returns error for null") {
        val actual = parse(null)
        assert(actual)(isLeft(equalTo("error - input string is 'null'")))
      },
      test("returns error for empty string") {
        val actual = parse("")
        assert(actual)(isLeft(equalTo("error with ''")))
      },
      test("returns error for for '.'") {
        val actual = parse(".")
        assert(actual)(isLeft(equalTo("error - at least one element must be specified")))
      },
      test("returns multiple errors - one for each element error") {
        val actual = parse("fo$o.ba$r[9].ba$z")
        assert(actual)(isLeft(equalTo("error with 'fo$o',error with 'ba$r[9]',error with 'ba$z'")))
      },
      test("""'foo[X]' returns Left("error with 'foo[X]'")""") {
        val actual = parse("foo[X]")
        assert(actual)(isLeft(equalTo("error with 'foo[X]'")))
      }
    )
}
