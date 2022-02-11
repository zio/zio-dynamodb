package zio.dynamodb

import zio.Chunk
import zio.dynamodb.ConditionExpression.Operand.ProjectionExpressionOperand
import zio.dynamodb.DynamoDBQuery.toItem
import zio.dynamodb.ProjectionExpression.{ ListElement, MapElement, Root }
import zio.dynamodb.UpdateExpression.SetOperand.{ IfNotExists, ListAppend, ListPrepend, PathOperand }
import zio.schema.Schema
import zio.schema.{ AccessorBuilder, Schema }

import scala.annotation.tailrec
import scala.annotation.implicitNotFound

// The maximum depth for a document path is 32
sealed trait ProjectionExpression { self =>
  type From
  type To

  def apply(index: Int): ProjectionExpression = ProjectionExpression.ListElement(self, index)

  def apply(key: String): ProjectionExpression = ProjectionExpression.MapElement(self, key)

  // ConditionExpression with another ProjectionExpression

  def ===(that: ProjectionExpression): ConditionExpression =
    ConditionExpression.Equals(
      ProjectionExpressionOperand(self),
      ConditionExpression.Operand.ProjectionExpressionOperand(that)
    )
  def <>(that: ProjectionExpression): ConditionExpression  =
    ConditionExpression.NotEqual(
      ProjectionExpressionOperand(self),
      ConditionExpression.Operand.ProjectionExpressionOperand(that)
    )
  def <(that: ProjectionExpression): ConditionExpression   =
    ConditionExpression.LessThan(
      ProjectionExpressionOperand(self),
      ConditionExpression.Operand.ProjectionExpressionOperand(that)
    )
  def <=(that: ProjectionExpression): ConditionExpression  =
    ConditionExpression.LessThanOrEqual(
      ProjectionExpressionOperand(self),
      ConditionExpression.Operand.ProjectionExpressionOperand(that)
    )
  def >(that: ProjectionExpression): ConditionExpression   =
    ConditionExpression.GreaterThanOrEqual(
      ProjectionExpressionOperand(self),
      ConditionExpression.Operand.ProjectionExpressionOperand(that)
    )
  def >=(that: ProjectionExpression): ConditionExpression  =
    ConditionExpression.GreaterThanOrEqual(
      ProjectionExpressionOperand(self),
      ConditionExpression.Operand.ProjectionExpressionOperand(that)
    )

  // unary ConditionExpressions

  def exists: ConditionExpression            = ConditionExpression.AttributeExists(self)
  def notExists: ConditionExpression         = ConditionExpression.AttributeNotExists(self)
  def size: ConditionExpression.Operand.Size = ConditionExpression.Operand.Size(self)

  def isBinary: ConditionExpression    = isType(AttributeValueType.Binary)
  def isNumber: ConditionExpression    = isType(AttributeValueType.Number)
  def isString: ConditionExpression    = isType(AttributeValueType.String)
  def isBool: ConditionExpression      = isType(AttributeValueType.Bool)
  def isBinarySet: ConditionExpression = isType(AttributeValueType.BinarySet)
  def isList: ConditionExpression      = isType(AttributeValueType.List)
  def isMap: ConditionExpression       = isType(AttributeValueType.Map)
  def isNumberSet: ConditionExpression = isType(AttributeValueType.NumberSet)
  def isNull: ConditionExpression      = isType(AttributeValueType.Null)
  def isStringSet: ConditionExpression = isType(AttributeValueType.StringSet)

  private def isType(attributeType: AttributeValueType): ConditionExpression =
    ConditionExpression.AttributeType(self, attributeType)

  // ConditionExpression with AttributeValue's

  // constraint = av must be a String OR a Set
  def contains[A](av: A)(implicit t: ToAttributeValue[A]): ConditionExpression =
    ConditionExpression.Contains(self, t.toAttributeValue(av))

  // t: needs to be more constrained than implicit t: ToAttributeValue[A] as function only applies to Strings
  def beginsWith[A](av: A)(implicit t: ToAttributeValue[A] /*, ev: RefersToString[To] */ ): ConditionExpression =
//    println(ev) // TODO to get around "parameter value ev in method beginsWith is never used"
    ConditionExpression.BeginsWith(self, t.toAttributeValue(av))

  def between[A](minValue: A, maxValue: A)(implicit t: ToAttributeValue[A]): ConditionExpression =
    ConditionExpression.Operand
      .ProjectionExpressionOperand(self)
      .between(t.toAttributeValue(minValue), t.toAttributeValue(maxValue))
  def in[A](values: Set[A])(implicit t: ToAttributeValue[A]): ConditionExpression                =
    ConditionExpression.Operand.ProjectionExpressionOperand(self).in(values.map(t.toAttributeValue))
  def in[A](value: A, values: A*)(implicit t: ToAttributeValue[A]): ConditionExpression          =
    ConditionExpression.Operand
      .ProjectionExpressionOperand(self)
      .in(values.map(t.toAttributeValue).toSet + t.toAttributeValue(value))

  def ===[A](that: A)(implicit t: ToAttributeValue[A]): ConditionExpression =
    ConditionExpression.Equals(
      ProjectionExpressionOperand(self),
      ConditionExpression.Operand.ValueOperand(t.toAttributeValue(that))
    )
  def <>[A](that: A)(implicit t: ToAttributeValue[A]): ConditionExpression  =
    ConditionExpression.NotEqual(
      ProjectionExpressionOperand(self),
      ConditionExpression.Operand.ValueOperand(t.toAttributeValue(that))
    )
  def <[A](that: A)(implicit t: ToAttributeValue[A]): ConditionExpression   =
    ConditionExpression.LessThan(
      ProjectionExpressionOperand(self),
      ConditionExpression.Operand.ValueOperand(t.toAttributeValue(that))
    )
  def <=[A](that: A)(implicit t: ToAttributeValue[A]): ConditionExpression  =
    ConditionExpression.LessThanOrEqual(
      ProjectionExpressionOperand(self),
      ConditionExpression.Operand.ValueOperand(t.toAttributeValue(that))
    )
  def >[A](that: A)(implicit t: ToAttributeValue[A]): ConditionExpression   =
    ConditionExpression.GreaterThanOrEqual(
      ProjectionExpressionOperand(self),
      ConditionExpression.Operand.ValueOperand(t.toAttributeValue(that))
    )
  def >=[A](that: A)(implicit t: ToAttributeValue[A]): ConditionExpression  =
    ConditionExpression.GreaterThanOrEqual(
      ProjectionExpressionOperand(self),
      ConditionExpression.Operand.ValueOperand(t.toAttributeValue(that))
    )

  // UpdateExpression conversions

  /**
   * Modify or Add an item Attribute
   */
  def setValue[A](a: A)(implicit t: ToAttributeValue[A]): UpdateExpression.Action.SetAction =
    UpdateExpression.Action.SetAction(self, UpdateExpression.SetOperand.ValueOperand(t.toAttributeValue(a)))

  def set[A: Schema](a: A): UpdateExpression.Action.SetAction = setValue(toItem(a))

  /**
   * Modify or Add an item Attribute
   */
  def set(pe: ProjectionExpression): UpdateExpression.Action.SetAction =
    UpdateExpression.Action.SetAction(self, PathOperand(pe))

  /**
   * Modifying or Add item Attributes if ProjectionExpression `pe` exists
   */
  def setIfNotExists[A](pe: ProjectionExpression, a: A)(implicit
    t: ToAttributeValue[A]
  ): UpdateExpression.Action.SetAction =
    UpdateExpression.Action.SetAction(self, IfNotExists(pe, t.toAttributeValue(a)))

  /**
   * Add list `xs` to the end of this PathExpression
   */
  def appendList[A](xs: Iterable[A])(implicit t: ToAttributeValue[A]): UpdateExpression.Action.SetAction =
    UpdateExpression.Action.SetAction(self, ListAppend(self, AttributeValue.List(xs.map(t.toAttributeValue))))

  /**
   * Add list `xs` to the beginning of this PathExpression
   */
  def prependList[A](xs: Iterable[A])(implicit t: ToAttributeValue[A]): UpdateExpression.Action.SetAction =
    UpdateExpression.Action.SetAction(self, ListPrepend(self, AttributeValue.List(xs.map(t.toAttributeValue))))

  /**
   * Updating Numbers and Sets
   */
  def add[A](a: A)(implicit t: ToAttributeValue[A]): UpdateExpression.Action.AddAction =
    UpdateExpression.Action.AddAction(self, t.toAttributeValue(a))

  /**
   * Removes this PathExpression from an item
   */
  def remove: UpdateExpression.Action.RemoveAction =
    UpdateExpression.Action.RemoveAction(self)

  /**
   * Delete Elements from a Set
   */
  def deleteFromSet[A](a: A)(implicit t: ToAttributeValue[A]): UpdateExpression.Action.DeleteAction =
    UpdateExpression.Action.DeleteAction(self, t.toAttributeValue(a))

  override def toString: String = {
    @tailrec
    def loop(pe: ProjectionExpression, acc: List[String]): List[String] =
      pe match {
        /*
        If you have a PE that DDB does not know how to handle, then you have an error
        eg [0] // DDB does not support top level array or primitives at the top level
        so we need more code everywhere it is used to check that ROOT is valid and maybe
        special case it when in the context of the top level.
         */
        case Root                                        => acc
        case ProjectionExpression.MapElement(Root, name) => acc :+ s"$name"
        case MapElement(parent, key)                     => loop(parent, acc :+ s".$key")
        case ListElement(parent, index)                  => loop(parent, acc :+ s"[$index]")
      }

    loop(self, List.empty).reverse.mkString("")
  }
}

@implicitNotFound("the type ${A} must be a string in order to use this operator")
sealed trait RefersToString[-A]
object RefersToString {
  implicit val x: RefersToString[String] = new RefersToString[String] {}
}

object ProjectionExpression {
  type Typed[From0, To0] = ProjectionExpression {
    type From = From0
    type To   = To0
  }

  val builder = new AccessorBuilder {
    override type Lens[From, To]      = ProjectionExpression.Typed[From, To]
    override type Prism[From, To]     = ProjectionExpression.Typed[From, To]
    override type Traversal[From, To] = Unit

    // ProjectionExpression.MapElement(Root, name)

    override def makeLens[S, A](product: Schema.Record[S], term: Schema.Field[A]): Lens[S, A] =
      ProjectionExpression.MapElement(Root, term.label).asInstanceOf[Lens[S, A]]
    //ProjectionExpression.Root(term.label).asInstanceOf[Lens[S, A]]

    /*
    need to respect enum annotations
    may need PE.identity case object => we do not need Root anymore

     */
    override def makePrism[S, A](sum: Schema.Enum[S], term: Schema.Case[A, S]): Prism[S, A] =
      ProjectionExpression.MapElement(Root, term.id).asInstanceOf[Prism[S, A]]
    //ProjectionExpression.Root(term.id).asInstanceOf[Prism[S, A]]

    override def makeTraversal[S, A](collection: Schema.Collection[S, A], element: Schema[A]): Traversal[S, A] = ()
  }

  // where should we put this?
  def accessors[A](implicit s: Schema[A]): s.Accessors[builder.Lens, builder.Prism, builder.Traversal] =
    s.makeAccessors(builder)

  final case class Person(name: String, age: Int)
  object Person {
    implicit val schema = Schema.CaseClass2[String, Int, Person](
      Schema.Field("name", Schema[String]),
      Schema.Field("age", Schema[Int]),
      Person(_, _),
      _.name,
      _.age
    )

    /*
    we only want this to work on Person

    where age > 2
     */
    val (name, age) = ProjectionExpression.accessors[Person]
//    age.beginsWith("X") // this will fail compilation with a custom compile error msg - nice!
    name.beginsWith("X")

    /*
    TODO
    propagate ROOT changes
    propagate operator constraint changes
    fix tests:
      [error] Failed tests:
      [error]         zio.dynamodb.AliasMapRenderSpec
      [error]         zio.dynamodb.ProjectionExpressionParserSpec
    next step is to deal with strings in path expressions
     */
  }

  private val regexMapElement     = """(^[a-zA-Z0-9_]+)""".r
  private val regexIndexedElement = """(^[a-zA-Z0-9_]+)(\[[0-9]+])+""".r
  private val regexGroupedIndexes = """(\[([0-9]+)])""".r

  // Note that you can only use a ProjectionExpression if the first character is a-z or A-Z and the second character
  // (if present) is a-z, A-Z, or 0-9. Also key words are not allowed
  // If this is not the case then you must use the Expression Attribute Names facility to create an alias.
  // Attribute names containing a dot "." must also use the Expression Attribute Names
  def apply(name: String): ProjectionExpression = ProjectionExpression.MapElement(Root, name)

  case object Root                                                       extends ProjectionExpression
  final case class MapElement(parent: ProjectionExpression, key: String) extends ProjectionExpression
  // index must be non negative - we could use a new type here?
  final case class ListElement(parent: ProjectionExpression, index: Int) extends ProjectionExpression

  /**
   * Unsafe version of `parse` that throws an exception rather than returning an Either
   * @see [[parse]]
   */
  def $(s: String): ProjectionExpression =
    parse(s) match {
      case Right(a)  => a
      case Left(msg) => throw new IllegalStateException(msg)
    }

  // TODO: Think about Expression Attribute Names for value substitution - do we need this?
  // TODO: eg "foo.#key.baz"
  // TODO: see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html
  /**
   * Parses a string into an ProjectionExpression
   * eg
   * {{{
   * parse("foo.bar[9].baz"")
   * // Right(MapElement(ListElement(MapElement(Root(bar),baz),9),baz))
   * parse(fo$$o.ba$$r[9].ba$$z)
   * // Left("error with fo$$o,error with ba$$r[9],error with ba$$z")
   * }}}
   * @param s Projection expression as a string
   * @return either a `Right` of ProjectionExpression if successful, else a `Chunk` of error strings
   */
  def parse(s: String): Either[String, ProjectionExpression] = {

    final case class Builder(pe: Option[Either[Chunk[String], ProjectionExpression]] = None) { self =>

      def mapElement(name: String): Builder =
        Builder(self.pe match {
          case None                     =>
            Some(Right(ProjectionExpression.MapElement(Root, name)))
          case Some(Right(pe))          =>
            Some(Right(pe(name)))
          case someLeft @ Some(Left(_)) =>
            someLeft
        })

      def listElement(name: String, indexes: List[Int]): Builder = {
        @tailrec
        def multiDimPe(pe: ProjectionExpression, indexes: List[Int]): ProjectionExpression =
          if (indexes == Nil)
            pe
          else
            multiDimPe(pe(indexes.head), indexes.tail)

        Builder(self.pe match {
          case None                     =>
            Some(Right(multiDimPe(ProjectionExpression.MapElement(Root, name), indexes)))
          case Some(Right(pe))          =>
            Some(Right(multiDimPe(MapElement(pe, name), indexes)))
          case someLeft @ Some(Left(_)) =>
            someLeft
        })
      }

      def addError(s: String): Builder =
        Builder(self.pe match {
          case None | Some(Right(_)) =>
            Some(Left(Chunk(s"error with '$s'")))
          case Some(Left(chunk))     =>
            Some(Left(chunk :+ s"error with '$s'"))
        })

      def either: Either[Chunk[String], ProjectionExpression] =
        self.pe.getOrElse(Left(Chunk("error - at least one element must be specified")))
    }

    if (s == null)
      Left("error - input string is 'null'")
    else if (s.startsWith(".") || s.endsWith("."))
      Left(s"error - input string '$s' is invalid")
    else {

      val elements: List[String] = s.split("\\.").toList

      val builder = elements.foldLeft(Builder()) {
        case (accBuilder, s) =>
          s match {
            case regexIndexedElement(name, _) =>
              val indexesString = s.substring(s.indexOf('['))
              val indexes       = regexGroupedIndexes.findAllMatchIn(indexesString).map(_.group(2).toInt).toList
              accBuilder.listElement(name, indexes)
            case regexMapElement(name)        =>
              accBuilder.mapElement(name)
            case _                            =>
              accBuilder.addError(s)
          }
      }

      builder.either.left.map(_.mkString(","))
    }
  }
}
