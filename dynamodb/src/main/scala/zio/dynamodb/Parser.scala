package zio.dynamodb

import zio.Chunk
import zio.dynamodb.ProjectionExpression.Root

object Parser extends App {
  /*
  final case class Root(name: String)                                    extends ProjectionExpression
  final case class MapElement(parent: ProjectionExpression, key: String) extends ProjectionExpression
  // index must be non negative - we could use a new type here?
  final case class ListElement(parent: ProjectionExpression, index: Int) extends ProjectionExpression


covert String to list
then recursion
$("one[2]")
$("foo.bar[9].baz")
   1  23  45678
$("foo.bar[9].baz")
"foo"


   */

  class PEBuilder() { self =>
    var pe: Option[ProjectionExpression] = None

    def addChildMap(s: String): PEBuilder = {
      pe = pe.fold[Option[ProjectionExpression]](Some(Root(s)))(pe => Some(pe(s)))
      self
    }

    def addChildArray(s: String, i: Int): PEBuilder = {
      pe = pe.fold[Option[ProjectionExpression]](Some(Root(s)))(pe => Some(pe(s))).map(_(i))
      self
    }

    def getPE: Option[ProjectionExpression] = pe
  }

  case class PEBuilder2(pe: Option[ProjectionExpression] = None) { self =>

    def addChildMap(s: String): PEBuilder2 =
      PEBuilder2(pe.fold[Option[ProjectionExpression]](Some(Root(s)))(pe => Some(pe(s))))

    def addChildArray(s: String, i: Int): PEBuilder2 =
      PEBuilder2(pe.fold[Option[ProjectionExpression]](Some(Root(s)))(pe => Some(pe(s))).map(_(i)))

    // TODO: remove
    def getPE: Option[ProjectionExpression] = pe
  }

  case class PEBuilder3(pe: Option[Either[Chunk[String], ProjectionExpression]] = None) { self =>

    def addChildMap(s: String): PEBuilder3 =
      PEBuilder3(pe match {
        case None                     =>
          Some(Right(Root(s)))
        case Some(Right(pe))          =>
          Some(Right(pe(s)))
        case someLeft @ Some(Left(_)) =>
          someLeft
      })

    def addChildArray(s: String, i: Int): PEBuilder3 =
      PEBuilder3(pe match {
        case None                     =>
          Some(Right(Root(s)))
        case Some(Right(pe))          =>
          Some(Right(pe(s)(i)))
        case someLeft @ Some(Left(_)) =>
          someLeft
      })

    def addError(s: String): PEBuilder3 =
      PEBuilder3(pe match {
        case None | Some(Right(_)) =>
          Some(Left(Chunk(s"error with $s")))
        case Some(Left(chunk))     =>
          Some(Left(chunk :+ s"error with $s"))
      })

    // TODO: remove
    def getPE: Option[Either[Chunk[String], ProjectionExpression]] = pe
  }

  val regexIndex = """(^[a-zA-Z_]+)\[([0-9]+)]""".r
  val regexMap   = """(^[a-zA-Z_]+)""".r

  def parse(s: String): Option[ProjectionExpression] = {

    val elements: List[String] = s.split("\\.").toList
    elements.foreach(println)

    val pe = elements.foldLeft(new PEBuilder()) {
      case (pe, s) =>
        s match {
          case regexIndex(name, index) =>
            pe.addChildArray(name, index.toInt)
          case regexMap(name)          =>
            pe.addChildMap(name)
          case _                       =>
            println("XXXXXXXXXXXXX No match! TODO: error!")
            pe
        }
    }

    pe.getPE
  }

  def parse2(s: String): Either[Chunk[String], ProjectionExpression] = {

    val elements: List[String] = s.split("\\.").toList
    elements.foreach(println)

    val pe = elements.foldLeft(PEBuilder3()) {
      case (pe, s) =>
        s match {
          case regexIndex(name, index) =>
            pe.addChildArray(name, index.toInt)
          case regexMap(name)          =>
            pe.addChildMap(name)
          case _                       =>
            pe.addError(s)
        }
    }

    val value: Either[Chunk[String], ProjectionExpression] = pe.getPE.getOrElse(Left(Chunk("Empty ProjectExpression")))
    value
  }

  val x  = parse("bar_$Foo.baz[9]")
  println(x)
  val x2 = parse2("$barFoo.baz[9].boom.b$umba")
  println(x2)

}
