package zio.dynamodb

import zio.dynamodb.ProjectionExpressionParser.{ $, parse }
import zio.test._

object ProjectionExpressionParserSpec extends ZIOSpecDefault {

  def spec = suite("ProjectionExpressionParser")(
    parseSuite,
    dollarSuite
  )

  // The ZIO Test assertTrue macro introspects expression types at compile time and
  // cannot unify ProjectionExpression.Unknown (an abstract type member) with its
  // existential form. These two helpers erase Unknown before assertTrue sees it.
  private def parseStr(s: String): Either[String, String] = parse(s).map(_.toString)
  private def parseErr(s: String): Option[String]         = parse(s).left.toOption

  // ---------------------------------------------------------------------------
  // parse
  // ---------------------------------------------------------------------------

  private val parseSuite = suite("parse")(
    suite("valid inputs")(
      test("simple name") {
        assertTrue(parseStr("foo") == Right("foo"))
      },
      test("nested dot path") {
        assertTrue(parseStr("a.b.c") == Right("a.b.c"))
      },
      test("list index") {
        assertTrue(parseStr("items[0]") == Right("items[0]"))
      },
      test("list index with non-zero index") {
        assertTrue(parseStr("items[3]") == Right("items[3]"))
      },
      test("multi-dimensional index") {
        assertTrue(parseStr("matrix[0][1]") == Right("matrix[0][1]"))
      },
      test("path with list index in the middle") {
        assertTrue(parseStr("items[2].name") == Right("items[2].name"))
      },
      test("deeply nested path with index") {
        assertTrue(parseStr("a.b[0].c") == Right("a.b[0].c"))
      },
      test("backtick-quoted name preserves backticks") {
        assertTrue(parseStr("`foo.bar`") == Right("`foo.bar`"))
      },
      test("backtick-quoted segment inside a path") {
        assertTrue(parseStr("a.`b.c`.d") == Right("a.`b.c`.d"))
      },
      test("underscore in name") {
        assertTrue(parseStr("foo_bar") == Right("foo_bar"))
      },
      test("hyphen in name") {
        assertTrue(parseStr("foo-bar") == Right("foo-bar"))
      },
      test("alphanumeric name") {
        assertTrue(parseStr("item2") == Right("item2"))
      }
    ),
    suite("invalid inputs")(
      test("null input") {
        assertTrue(parseErr(null).isDefined)
      },
      test("leading dot") {
        assertTrue(parseErr(".foo").isDefined)
      },
      test("trailing dot") {
        assertTrue(parseErr("foo.").isDefined)
      },
      test("invalid character") {
        assertTrue(parseErr("foo$bar").isDefined)
      },
      test("all errors across segments are collected") {
        val msg = parseErr("valid.fo$$o.bar$$").getOrElse("")
        assertTrue(msg.contains("fo$$o") && msg.contains("bar$$"))
      },
      test("single invalid segment reports the bad token") {
        val msg = parseErr("fo$$o").getOrElse("")
        assertTrue(msg.contains("fo$$o"))
      }
    )
  )

  // ---------------------------------------------------------------------------
  // $
  // ---------------------------------------------------------------------------

  private val dollarSuite = suite("$")(
    test("simple name") {
      assertTrue($("name").toString == "name")
    },
    test("nested path") {
      assertTrue($("a.b.c").toString == "a.b.c")
    },
    test("list index") {
      assertTrue($("items[1]").toString == "items[1]")
    },
    test("multi-dimensional index") {
      assertTrue($("matrix[0][2]").toString == "matrix[0][2]")
    },
    test("path with list index in the middle") {
      assertTrue($("person.addresses[0].street").toString == "person.addresses[0].street")
    },
    test("backtick-quoted name") {
      assertTrue($("`foo.bar`").toString == "`foo.bar`")
    },
    test("throws IllegalStateException for invalid input") {
      assertTrue(scala.util.Try($("$$invalid")).isFailure)
    },
    test("throws IllegalStateException for null") {
      assertTrue(scala.util.Try($(null)).isFailure)
    }
  )
}
