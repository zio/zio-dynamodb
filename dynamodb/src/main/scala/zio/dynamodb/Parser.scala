package zio.dynamodb

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
            println("No match! TODO: error!")
            pe
        }
    }

    pe.getPE
  }

  val x = parse("bar_Foo.baz[9]")
  println(x)

}
