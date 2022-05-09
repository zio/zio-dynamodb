package zio.dynamodb

import zio.dynamodb.ProjectionExpression.{ $, parse, ListElement, MapElement, Root }
import zio.random.Random
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, _ }

import scala.annotation.tailrec

object ProjectionExpressionParserSpec extends DefaultRunnableSpec {
  object Generators {
    private val maxFields                                                                                          = 20
    private val validCharGens                                                                                      = List(Gen.const('_'), Gen.char('a', 'z'), Gen.char('A', 'Z'), Gen.char('0', '9'))
    private def fieldName                                                                                          = Gen.stringBounded(0, 10)(Gen.oneOf(validCharGens: _*))
    private def index                                                                                              = Gen.int(0, 10)
    private def root: Gen[Random with Sized, ProjectionExpression[_]]                                              =
      fieldName.map(ProjectionExpression.MapElement(Root, _))
    private def mapElement(parent: => ProjectionExpression[_])                                                     = fieldName.map(MapElement(parent, _))
    private def listElement(parent: => ProjectionExpression[_])                                                    = index.map(ListElement(parent, _))
    private def mapOrListElement(parent: ProjectionExpression[_]): Gen[Random with Sized, ProjectionExpression[_]] =
      Gen.oneOf(mapElement(parent), listElement(parent))

    def projectionExpression: Gen[Random with Sized, ProjectionExpression[_]] = {
      @tailrec
      def loop(
        parentGen: Gen[Random with Sized, ProjectionExpression[_]],
        counter: Int
      ): Gen[Random with Sized, ProjectionExpression[_]] =
        if (counter == 0)
          parentGen
        else
          loop(parentGen.flatMap(pe => mapOrListElement(pe)), counter - 1)

      for {
        count <- Gen.int(1, maxFields)
        pe    <- loop(root, count)
      } yield pe
    }

  }

  override def spec: ZSpec[Environment, Failure] = mainSuite

  private val mainSuite =
    suite("ProjectionExpression Parser")(
      test("$ function compiles") {
        val _ = $("name").beginsWith("Avi")
        assertCompletes
      },
      testM("should parse valid expressions and return a Left for any invalid expressions") {
        check(Generators.projectionExpression) { pe =>
          assert(parse(pe.toString))(
            if (anyEmptyName(pe)) isLeft
            else isRight(equalTo(pe))
          )
        }
      },
      test("toString on a ProjectionExpression of a_0[0]") {
        assert(parse("a_0[0]"))(isRight)
      },
      test("toString on a ProjectionExpression of foo.bar[9].baz") {
        val pe = MapElement(ListElement(MapElement(Root("foo"), "bar"), 9), "baz")
        assert(pe.toString)(equalTo("foo.bar[9].baz"))
      },
      test("returns error for null") {
        val actual = parse(null)
        assert(actual)(isLeft(equalTo("error - input string is 'null'")))
      },
      test("returns error for empty string") {
        val actual = parse("")
        assert(actual)(isLeft(equalTo("error with ''")))
      },
      test("returns error for '.'") {
        val actual = parse(".")
        assert(actual)(isLeft(equalTo("error - input string '.' is invalid")))
      },
      test("returns error for for 'foo.'") {
        val actual = parse("foo.")
        assert(actual)(isLeft(equalTo("error - input string 'foo.' is invalid")))
      },
      test("returns error for '.foo'") {
        val actual = parse(".foo")
        assert(actual)(isLeft(equalTo("error - input string '.foo' is invalid")))
      },
      test("returns error for for 'foo..bar'") {
        val actual = parse("foo..bar")
        assert(actual)(isLeft(equalTo("error with ''")))
      },
      test("returns multiple errors - one for each element error") {
        val actual = parse("fo$o.ba$r[9].ba$z")
        assert(actual)(isLeft(equalTo("error with 'fo$o',error with 'ba$r[9]',error with 'ba$z'")))
      },
      test("Non number index returns an error") {
        val actual = parse("foo[X]")
        assert(actual)(isLeft(equalTo("error with 'foo[X]'")))
      }
    )

  @tailrec
  private def anyEmptyName(pe: ProjectionExpression[_]): Boolean =
    pe match {
      case Root                                        =>
        throw new IllegalStateException(s"$pe is a Root, this should never happen")
      case ProjectionExpression.MapElement(Root, name) =>
        name.isEmpty
      case MapElement(parent, key)                     =>
        key.isEmpty || anyEmptyName(parent)
      case ListElement(parent, _)                      =>
        anyEmptyName(parent)
    }

}
