package zio.dynamodb

import scala.annotation.tailrec

object ProjectionExpressionParser {

  import ProjectionExpression._

  // Splits on dots that are outside backtick-escaped segments.
  // Positive lookahead ensures an even number of backticks follow the dot.
  private val splitOnDot = """\.(?=(?:[^`]*`[^`]*`)*(?![^`]*`))""".r

  // Matches an entire segment that is a bare name (alphanumeric / underscore / hyphen)
  // or a backtick-quoted name (allows dots and other special chars inside).
  private val bareSegment = """^([a-zA-Z0-9_-]+|`[^`]+`)$""".r

  // Same name prefix followed by one or more [N] index suffixes.
  private val indexedSegment = """^([a-zA-Z0-9_-]+|`[^`]+`)(\[[0-9]+])+$""".r

  // Captures the numeric value inside each [N] group.
  private val captureIndexes = """\[([0-9]+)]""".r

  /**
   * Parses a dot-notation path string into a ProjectionExpression.
   * Backtick-quoted segments allow dots and other special characters in attribute names.
   * All errors across all path segments are collected and returned together.
   *
   * {{{
   * parse("foo")            // Right(MapElement(Root, "foo"))
   * parse("a.b.c")          // Right: a.b.c
   * parse("items[0].name")  // Right: items[0].name
   * parse("matrix[0][1]")   // Right: matrix[0][1]
   * parse("`foo.bar`")      // Right(MapElement(Root, "`foo.bar`"))
   * }}}
   */
  private[dynamodb] def parse(s: String): Either[String, ProjectionExpression[Unknown, Unknown]] = {
    if (s == null)
      return Left("error - input string is 'null'")
    if (s.startsWith(".") || s.endsWith("."))
      return Left(s"error - '$s' must not start or end with '.'")

    type PE = ProjectionExpression[Unknown, Unknown]

    val result: Either[List[String], PE] =
      splitOnDot.split(s).toList.foldLeft[Either[List[String], PE]](Right(Root.asInstanceOf[PE])) { case (acc, seg) =>
        val step: Either[String, PE => PE] = seg match {
          case indexedSegment(name, _) =>
            val indexes = captureIndexes
              .findAllMatchIn(seg.substring(seg.indexOf('[')))
              .map(_.group(1).toInt)
              .toList
            Right(parent => applyIndexes(MapElement(parent, name), indexes))
          case bareSegment(name)       =>
            Right(parent => MapElement(parent, name))
          case _                       =>
            Left(s"error with '$seg'")
        }
        (acc, step) match {
          case (Right(pe), Right(f)) => Right(f(pe))
          case (Right(_), Left(e))   => Left(List(e))
          case (Left(es), Right(_))  => Left(es)
          case (Left(es), Left(e))   => Left(es :+ e)
        }
      }

    result.left.map(_.mkString(", "))
  }

  /**
   * Unsafe string parser — throws [[IllegalStateException]] on invalid input.
   * Use [[parse]] for a safe alternative.
   *
   * Backtick-quoted segments allow dots and other special chars in attribute names.
   * Underscore and hyphen are allowed unquoted.
   *
   * {{{
   * $("foo")            // simple attribute
   * $("a.b.c")          // nested path
   * $("items[0].name")  // list element with nested access
   * $("matrix[0][1]")   // multi-dimensional list index
   * $("`foo.bar`")      // attribute name containing a dot
   * }}}
   */
  private[dynamodb] def $(s: String): ProjectionExpression[Any, Unknown] =
    parse(s) match {
      case Right(pe) => pe.asInstanceOf[ProjectionExpression[Any, Unknown]]
      case Left(msg) => throw new IllegalStateException(msg)
    }

  // Applies a sequence of list indexes on top of a base expression, left to right.
  private def applyIndexes(
    base: ProjectionExpression[Unknown, Unknown],
    indexes: List[Int]
  ): ProjectionExpression[Unknown, Unknown] = {
    @tailrec
    def loop(pe: ProjectionExpression[Unknown, Unknown], remaining: List[Int]): ProjectionExpression[Unknown, Unknown] =
      remaining match {
        case Nil          => pe
        case head :: tail => loop(ListElement(pe, head), tail)
      }
    loop(base, indexes)
  }
}
