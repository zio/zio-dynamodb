package zio.dynamodb

import zio.Chunk
import zio.dynamodb.ConditionExpression.Operand.ProjectionExpressionOperand
import zio.dynamodb.UpdateExpression.SetOperand.{ IfNotExists, ListAppend, ListPrepend, PathOperand }

// The maximum depth for a document path is 32
sealed trait ProjectionExpression { self =>
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

  def isType(attributeType: AttributeValueType): ConditionExpression =
    ConditionExpression.AttributeType(self, attributeType)

  // ConditionExpression with AttributeValue's

  def contains[A](av: A)(implicit t: ToAttributeValue[A]): ConditionExpression                   =
    ConditionExpression.Contains(self, t.toAttributeValue(av))
  def beginsWith[A](av: A)(implicit t: ToAttributeValue[A]): ConditionExpression                 =
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

  def set[A](a: A)(implicit t: ToAttributeValue[A]): UpdateExpression.Action.SetAction                       =
    UpdateExpression.Action.SetAction(self, UpdateExpression.SetOperand.ValueOperand(t.toAttributeValue(a)))
  def set(pe: ProjectionExpression): UpdateExpression.Action.SetAction                                       =
    UpdateExpression.Action.SetAction(self, PathOperand(pe))
  def setIfNotExists[A](pe: ProjectionExpression, a: A)(implicit
    t: ToAttributeValue[A]
  ): UpdateExpression.Action.SetAction                                                                       =
    UpdateExpression.Action.SetAction(self, IfNotExists(pe, t.toAttributeValue(a)))
  def setListAppend[A](xs: Iterable[A])(implicit t: ToAttributeValue[A]): UpdateExpression.Action.SetAction  =
    UpdateExpression.Action.SetAction(self, ListAppend(AttributeValue.List(xs.map(t.toAttributeValue))))
  def setListPrepend[A](xs: Iterable[A])(implicit t: ToAttributeValue[A]): UpdateExpression.Action.SetAction =
    UpdateExpression.Action.SetAction(self, ListPrepend(AttributeValue.List(xs.map(t.toAttributeValue))))
  def add[A](a: A)(implicit t: ToAttributeValue[A]): UpdateExpression.Action.AddAction                       =
    UpdateExpression.Action.AddAction(self, t.toAttributeValue(a))
  def remove: UpdateExpression.Action.RemoveAction                                                           =
    UpdateExpression.Action.RemoveAction(self)
  def delete[A](a: A)(implicit t: ToAttributeValue[A]): UpdateExpression.Action.DeleteAction                 =
    UpdateExpression.Action.DeleteAction(self, t.toAttributeValue(a))

}

object ProjectionExpression {
  // Note that you can only use a ProjectionExpression if the first character is a-z or A-Z and the second character
  // (if present) is a-z, A-Z, or 0-9. Also key words are not allowed
  // If this is not the case then you must use the Expression Attribute Names facility to create an alias.
  // Attribute names containing a dot "." must also use the Expression Attribute Names
  def apply(name: String): ProjectionExpression = Root(name)

  final case class Root(name: String)                                    extends ProjectionExpression
  final case class MapElement(parent: ProjectionExpression, key: String) extends ProjectionExpression
  // index must be non negative - we could use a new type here?
  final case class ListElement(parent: ProjectionExpression, index: Int) extends ProjectionExpression

  /**
   * Parses a string into an ProjectionExpression
   * eg
   * {{{
   * parse("foo.bar[9].baz"")
   * // Right(MapElement(ListElement(MapElement(Root(bar),baz),9),baz))
   * parse(fo$o.ba$r[9].ba$z)
   * // Left("error with fo$o,error with ba$r[9],error with ba$z")
   * }}}
   * @param s Projection expression as a string
   * @return either a `Right` of ProjectionExpression if successful, else a `Chunk` of error strings
   */
  def parse(s: String): Either[String, ProjectionExpression] = {

    class Builder(pe: Option[Either[Chunk[String], ProjectionExpression]] = None) {

      def addChildMap(s: String): Builder =
        new Builder(pe match {
          case None                     =>
            Some(Right(Root(s)))
          case Some(Right(pe))          =>
            Some(Right(pe(s)))
          case someLeft @ Some(Left(_)) =>
            someLeft
        })

      def addChildArray(s: String, i: Int): Builder =
        new Builder(pe match {
          case None                     =>
            Some(Right(Root(s)))
          case Some(Right(pe))          =>
            Some(Right(pe(s)(i)))
          case someLeft @ Some(Left(_)) =>
            someLeft
        })

      def addError(s: String): Builder =
        new Builder(pe match {
          case None | Some(Right(_)) =>
            Some(Left(Chunk(s"error with '$s'")))
          case Some(Left(chunk))     =>
            Some(Left(chunk :+ s"error with '$s'"))
        })

      def get: Either[Chunk[String], ProjectionExpression] =
        pe.getOrElse(Left(Chunk("error - at least one element must be specified")))
    }

    val regexIndex = """(^[a-zA-Z_]+)\[([0-9]+)]""".r
    val regexMap   = """(^[a-zA-Z_]+)""".r

    val elements: List[String] = s.split("\\.").toList

    val pe: Builder = elements.foldLeft(new Builder()) {
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

    pe.get.left.map(_.mkString(","))
  }
}
