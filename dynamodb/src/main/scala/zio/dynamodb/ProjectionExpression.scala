package zio.dynamodb

import zio.Chunk
import zio.dynamodb.ConditionExpression.Operand.ProjectionExpressionOperand
import zio.dynamodb.ProjectionExpression.{ ListElement, MapElement, Root }
import zio.dynamodb.UpdateExpression.SetOperand.{ IfNotExists, ListAppend, ListPrepend, PathOperand }
import zio.schema.{ AccessorBuilder, Schema }

import scala.annotation.{ implicitNotFound, tailrec }

// The maximum depth for a document path is 32
sealed trait ProjectionExpression[To] { self =>
  type From

  def unsafeTo[To2]: ProjectionExpression.Typed[From, To2] = self.asInstanceOf[ProjectionExpression.Typed[From, To2]]

  def apply(index: Int): ProjectionExpression[_] = ProjectionExpression.ListElement(self, index)

  def apply(key: String): ProjectionExpression[_] = ProjectionExpression.MapElement(self, key)

  // unary ConditionExpressions

  // applies to all types
  def exists: ConditionExpression    = ConditionExpression.AttributeExists(self)
  // applies to all types
  def notExists: ConditionExpression = ConditionExpression.AttributeNotExists(self)
  // Applies to all types except Number and Boolean
  def size(implicit ev: Sizable[To]): ConditionExpression.Operand.Size = {
    val _ = ev
    ConditionExpression.Operand.Size(self)
  }

  /**
   * Removes an element at the specified index from a list. Note that index is zero based
   */
  def remove(index: Int)(implicit ev: ListRemoveable[To]): UpdateExpression.Action.RemoveAction = {
    val _ = ev
    UpdateExpression.Action.RemoveAction(ProjectionExpression.ListElement(self, index))
  }

  // apply to all types
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

  private def isType(attributeType: AttributeValueType): ConditionExpression = // TODO: private so move down
    ConditionExpression.AttributeType(self, attributeType)

  /**
   * Only applies to a string attribute
   */
  def beginsWith(av: String)(implicit ev: RefersTo[String, To]): ConditionExpression = {
    val _ = ev
    ConditionExpression.BeginsWith(self, AttributeValue.String(av))
  }

  // UpdateExpression conversions

  /**
   * Removes this PathExpression from an item
   */
  def remove: UpdateExpression.Action.RemoveAction =
    UpdateExpression.Action.RemoveAction(self)

  override def toString: String = {
    @tailrec
    def loop(pe: ProjectionExpression[_], acc: List[String]): List[String] =
      pe match {
        case Root                                        => acc // identity
        case ProjectionExpression.MapElement(Root, name) => acc :+ s"$name"
        case MapElement(parent, key)                     => loop(parent, acc :+ s".$key")
        case ListElement(parent, index)                  => loop(parent, acc :+ s"[$index]")
      }

    loop(self, List.empty).reverse.mkString("")
  }
}

@implicitNotFound("the type ${X} is not ListRemoveable")
sealed trait ListRemoveable[-X]
trait ListRemoveable0 extends ListRemoveable1 {
  implicit def unknownRight[X]: ListRemoveable[ProjectionExpression.Unknown] =
    new ListRemoveable[ProjectionExpression.Unknown] {}
}
trait ListRemoveable1 {
  implicit def list[A]: ListRemoveable[Seq[A]] = new ListRemoveable[Seq[A]] {}
}
object ListRemoveable extends ListRemoveable0 {}

@implicitNotFound("the type ${X} is not Sizable")
sealed trait Sizable[-X]
trait SizableLowPriorityImplicits0 extends SizableLowPriorityImplicits1 {
  implicit def unknown: Sizable[ProjectionExpression.Unknown] =
    new Sizable[ProjectionExpression.Unknown] {}
}
trait SizableLowPriorityImplicits1 {
  implicit def iterable[A]: Sizable[Iterable[A]] = new Sizable[Iterable[A]] {}
  implicit def string[A]: Sizable[String]        = new Sizable[String] {}
}
object Sizable                     extends SizableLowPriorityImplicits0 {}

@implicitNotFound("the type ${A} must be a ${X} in order to use this operator")
sealed trait Addable[X, -A]
trait AddableLowPriorityImplicits0 extends AddableLowPriorityImplicits1 {
  implicit def unknownRight[X]: Addable[X, ProjectionExpression.Unknown] =
    new Addable[X, ProjectionExpression.Unknown] {}
}
trait AddableLowPriorityImplicits1 {
  implicit def set[A]: Addable[Set[A], A]      = new Addable[Set[A], A] {}
  implicit def int: Addable[Int, Int]          = new Addable[Int, Int] {}
  implicit def long: Addable[Long, Long]       = new Addable[Long, Long] {}
  implicit def float: Addable[Float, Float]    = new Addable[Float, Float] {}
  implicit def double: Addable[Double, Double] = new Addable[Double, Double] {}
  implicit def short: Addable[Short, Short]    = new Addable[Short, Short] {}
}
object Addable                     extends AddableLowPriorityImplicits0 {
  implicit def unknownLeft[X]: Addable[ProjectionExpression.Unknown, X] =
    new Addable[ProjectionExpression.Unknown, X] {}
}

@implicitNotFound("the type ${A} must be a ${X} in order to use this operator")
sealed trait Containable[X, -A]
trait ContainableLowPriorityImplicits0 extends ContainableLowPriorityImplicits1 {
  implicit def unknownRight[X]: Containable[X, ProjectionExpression.Unknown] =
    new Containable[X, ProjectionExpression.Unknown] {}
}
trait ContainableLowPriorityImplicits1 {
  implicit def set[A]: Containable[Set[A], A]      = new Containable[Set[A], A] {}
  implicit def string: Containable[String, String] = new Containable[String, String] {}
}
object Containable                     extends ContainableLowPriorityImplicits0 {
  implicit def unknownLeft[X]: Containable[ProjectionExpression.Unknown, X] =
    new Containable[ProjectionExpression.Unknown, X] {}
}

@implicitNotFound("the type ${A} must be a ${X} in order to use this operator")
sealed trait RefersTo[X, -A]
trait RefersToLowerPriorityImplicits0 extends RefersToLowerPriorityImplicits1 {
  implicit def unknownRight[X]: RefersTo[X, ProjectionExpression.Unknown] =
    new RefersTo[X, ProjectionExpression.Unknown] {}
}
trait RefersToLowerPriorityImplicits1 {
  implicit def identity[A]: RefersTo[A, A] = new RefersTo[A, A] {}
}
object RefersTo                       extends RefersToLowerPriorityImplicits0 {
  implicit def unknownLeft[X]: RefersTo[ProjectionExpression.Unknown, X] =
    new RefersTo[ProjectionExpression.Unknown, X] {}
}

trait ProjectionExpressionLowPriorityImplicits0 extends ProjectionExpressionLowPriorityImplicits1 {

  implicit class ProjectionExpressionSyntax0[To: ToAttributeValue](self: ProjectionExpression[To]) {
    def set(a: To): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(
        self,
        UpdateExpression.SetOperand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(a))
      )

    def set(pe: ProjectionExpression[To]): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(self, PathOperand(pe))

    /**
     *  Set attribute if it does not exists
     */
    def setIfNotExists(a: To): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(self, IfNotExists(self, implicitly[ToAttributeValue[To]].toAttributeValue(a)))

    /**
     * Append `a` to this list attribute
     */
    def append[A](a: A)(implicit ev: To <:< Iterable[A], to: ToAttributeValue[A]): UpdateExpression.Action.SetAction =
      appendList(List(a).asInstanceOf[To])

    /**
     * Add list `xs` to the end of this list attribute
     */
    def appendList[A](
      xs: To
    )(implicit ev: To <:< Iterable[A], to: ToAttributeValue[A]): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(
        self,
        ListAppend(self, AttributeValue.List(xs.map(a => to.toAttributeValue(a))))
      )

    /**
     * Prepend `a` to this list attribute
     */
    def prepend[A](a: A)(implicit ev: To <:< Iterable[A], to: ToAttributeValue[A]): UpdateExpression.Action.SetAction =
      prependList(List(a).asInstanceOf[To])

    /**
     * Add list `xs` to the beginning of this list attribute
     */
    def prependList[A](
      xs: To
    )(implicit ev: To <:< Iterable[A], to: ToAttributeValue[A]): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(
        self,
        ListPrepend(self, AttributeValue.List(xs.map(a => to.toAttributeValue(a))))
      )

    def between(minValue: To, maxValue: To): ConditionExpression =
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .between(
          implicitly[ToAttributeValue[To]].toAttributeValue(minValue),
          implicitly[ToAttributeValue[To]].toAttributeValue(maxValue)
        )

    /**
     * Remove all elements of parameter `set` from this set attribute
     */
    def deleteFromSet(set: To)(implicit ev: To <:< Set[_]): UpdateExpression.Action.DeleteAction =
      UpdateExpression.Action.DeleteAction(self, implicitly[ToAttributeValue[To]].toAttributeValue(set))

    def inSet(values: Set[To]): ConditionExpression =
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .in(values.map(implicitly[ToAttributeValue[To]].toAttributeValue))

    def in(value: To, values: To*): ConditionExpression = {
      val set: Set[To] = values.toSet + value
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .in(set.map(implicitly[ToAttributeValue[To]].toAttributeValue))
    }

    /**
     * Applies to a String or Set
     */
    def contains[A](av: A)(implicit ev: Containable[To, A], to: ToAttributeValue[A]): ConditionExpression = {
      val _ = ev
      ConditionExpression.Contains(self, to.toAttributeValue(av))
    }

    /**
     * adds this value as a number attribute if it does not exists, else adds the numeric value to the existing attribute
     */
    def add(a: To)(implicit ev: Addable[To, To]): UpdateExpression.Action.AddAction = {
      val _ = ev
      UpdateExpression.Action.AddAction(self, implicitly[ToAttributeValue[To]].toAttributeValue(a))
    }

    /**
     * adds this set as an attribute if it does not exists, else if it exists it adds the elements of the set
     */
    def addSet[A](
      set: Set[A]
    )(implicit ev: Addable[To, A], evSet: To <:< Set[A]): UpdateExpression.Action.AddAction = {
      val _ = ev
      UpdateExpression.Action.AddAction(self, implicitly[ToAttributeValue[To]].toAttributeValue(set.asInstanceOf[To]))
    }

    def ===(that: To): ConditionExpression =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )

    def <>(that: To): ConditionExpression =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )

    def <(that: To): ConditionExpression =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )

    def <=(that: To): ConditionExpression =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )

    def >(that: To): ConditionExpression =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )

    def >=(that: To): ConditionExpression =
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )

  }
}
trait ProjectionExpressionLowPriorityImplicits1 {
  implicit class ProjectionExpressionSyntax1[To](self: ProjectionExpression[To]) {
    def set[To2](
      that: ProjectionExpression[To]
    )(implicit refersTo: RefersTo[To, To2]): UpdateExpression.Action.SetAction = {
      val _ = refersTo
      UpdateExpression.Action.SetAction(self, PathOperand(that))
    }

    def ===[To2](that: ProjectionExpression[To2])(implicit refersTo: RefersTo[To, To2]): ConditionExpression = {
      val _ = refersTo
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )
    }
    def <>[To2](that: ProjectionExpression[To2])(implicit refersTo: RefersTo[To, To2]): ConditionExpression = {
      val _ = refersTo
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )
    }
    def <[To2](that: ProjectionExpression[To2])(implicit refersTo: RefersTo[To, To2]): ConditionExpression = {
      val _ = refersTo
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )
    }
    def <=[To2](that: ProjectionExpression[To2])(implicit refersTo: RefersTo[To, To2]): ConditionExpression = {
      val _ = refersTo
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )
    }
    def >[To2](that: ProjectionExpression[To2])(implicit refersTo: RefersTo[To, To2]): ConditionExpression = {
      val _ = refersTo
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )
    }
    def >=[To2](that: ProjectionExpression[To2])(implicit refersTo: RefersTo[To, To2]): ConditionExpression = {
      val _ = refersTo
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )
    }
  }
}

object ProjectionExpression extends ProjectionExpressionLowPriorityImplicits0 {

  type Unknown

  type Untyped = ProjectionExpression[_]

  type Typed[From0, To0] = ProjectionExpression[To0] {
    type From = From0
  }

  implicit class ProjectionExpressionSyntax(self: ProjectionExpression[Unknown]) {

    /**
     * Modify or Add an item Attribute
     */
    def set[To: ToAttributeValue](a: To): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(
        self,
        UpdateExpression.SetOperand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(a))
      )

    /**
     * Modify or Add an item Attribute
     */
    def set(that: ProjectionExpression[_]): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(self, PathOperand(that))

    /**
     * Add item attribute if it does not exists
     */
    def setIfNotExists[To: ToAttributeValue](a: To): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(self, IfNotExists(self, implicitly[ToAttributeValue[To]].toAttributeValue(a)))

    /**
     * Add item attribute if it does not exists
     */
    def setIfNotExists[To: ToAttributeValue](that: ProjectionExpression[_], a: To): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(self, IfNotExists(that, implicitly[ToAttributeValue[To]].toAttributeValue(a)))

    /**
     * Add list `xs` to the end of this list attribute
     */
    def appendList[To: ToAttributeValue](xs: Iterable[To]): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(
        self,
        ListAppend(self, AttributeValue.List(xs.map(a => implicitly[ToAttributeValue[To]].toAttributeValue(a))))
      )

    /**
     * Prepend `a` to this list attribute
     */
    def prepend[To: ToAttributeValue](a: To): UpdateExpression.Action.SetAction =
      prependList(List(a))

    /**
     * Add list `xs` to the beginning of this list attribute
     */
    def prependList[To: ToAttributeValue](xs: Iterable[To]): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(
        self,
        ListPrepend(self, AttributeValue.List(xs.map(a => implicitly[ToAttributeValue[To]].toAttributeValue(a))))
      )

    def between[To](minValue: To, maxValue: To)(implicit to: ToAttributeValue[To]): ConditionExpression =
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .between(to.toAttributeValue(minValue), to.toAttributeValue(maxValue))

    /**
     * Remove all elements of parameter "set" from this set
     */
    def deleteFromSet[To](
      set: To
    )(implicit ev: To <:< Set[_], to: ToAttributeValue[To]): UpdateExpression.Action.DeleteAction =
      UpdateExpression.Action.DeleteAction(self, to.toAttributeValue(set))

    def inSet[To](values: Set[To])(implicit to: ToAttributeValue[To]): ConditionExpression =
      ConditionExpression.Operand.ProjectionExpressionOperand(self).in(values.map(to.toAttributeValue))

    def in[To](value: To, values: To*)(implicit to: ToAttributeValue[To]): ConditionExpression = {
      val set: Set[To] = values.toSet + value
      ConditionExpression.Operand.ProjectionExpressionOperand(self).in(set.map(to.toAttributeValue))
    }

    /**
     * Applies to a String or Set
     */
    def contains[To](av: To)(implicit to: ToAttributeValue[To]): ConditionExpression =
      ConditionExpression.Contains(self, to.toAttributeValue(av))

    /**
     * adds a number attribute if it does not exists, else adds the numeric value to the existing attribute
     */
    def add[To](a: To)(implicit to: ToAttributeValue[To]): UpdateExpression.Action.AddAction =
      UpdateExpression.Action.AddAction(self, to.toAttributeValue(a))

    /**
     * adds a set attribute if it does not exists, else if it exists it adds the elements of the set
     */
    def addSet[To: ToAttributeValue](set: To)(implicit ev: To <:< Set[_]): UpdateExpression.Action.AddAction = {
      val _ = ev
      UpdateExpression.Action.AddAction(self, implicitly[ToAttributeValue[To]].toAttributeValue(set))
    }

    def ===[To: ToAttributeValue](that: To): ConditionExpression =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )

    def ===(that: ProjectionExpression[_]): ConditionExpression =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def <>[To: ToAttributeValue](that: To): ConditionExpression =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def <>(that: ProjectionExpression[_]): ConditionExpression  =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def <[To: ToAttributeValue](that: To): ConditionExpression =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def <(that: ProjectionExpression[_]): ConditionExpression  =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def <=[To: ToAttributeValue](that: To): ConditionExpression =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def <=(that: ProjectionExpression[_]): ConditionExpression  =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def >[To: ToAttributeValue](that: To): ConditionExpression =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def >(that: ProjectionExpression[_]): ConditionExpression  =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def >=[To: ToAttributeValue](that: To): ConditionExpression =
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def >=(that: ProjectionExpression[_]): ConditionExpression  =
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )
  }

  val builder = new AccessorBuilder {
    override type Lens[From, To]      = ProjectionExpression.Typed[From, To]
    override type Prism[From, To]     = ProjectionExpression.Typed[From, To]
    override type Traversal[From, To] = Unit

    override def makeLens[S, A](product: Schema.Record[S], term: Schema.Field[A]): Lens[S, A] =
      ProjectionExpression.MapElement(Root, term.label).asInstanceOf[Lens[S, A]]

    /*
    need to respect enum annotations
    may need PE.identity case object => we do not need Root anymore
     */
    override def makePrism[S, A](sum: Schema.Enum[S], term: Schema.Case[A, S]): Prism[S, A] =
      ProjectionExpression.MapElement(Root, term.id).asInstanceOf[Prism[S, A]]

    override def makeTraversal[S, A](collection: Schema.Collection[S, A], element: Schema[A]): Traversal[S, A] = ()
  }

  // where should we put this?
  def accessors[A](implicit s: Schema[A]): s.Accessors[builder.Lens, builder.Prism, builder.Traversal] =
    s.makeAccessors(builder)

  private val regexMapElement     = """(^[a-zA-Z0-9_]+)""".r
  private val regexIndexedElement = """(^[a-zA-Z0-9_]+)(\[[0-9]+])+""".r
  private val regexGroupedIndexes = """(\[([0-9]+)])""".r

  // Note that you can only use a ProjectionExpression if the first character is a-z or A-Z and the second character
  // (if present) is a-z, A-Z, or 0-9. Also key words are not allowed
  // If this is not the case then you must use the Expression Attribute Names facility to create an alias.
  // Attribute names containing a dot "." must also use the Expression Attribute Names
  def apply(name: String): ProjectionExpression[_] = ProjectionExpression.MapElement(Root, name)

  case object Root                                                              extends ProjectionExpression[Any]
  final case class MapElement[To](parent: ProjectionExpression[_], key: String) extends ProjectionExpression[To]
  // index must be non negative - we could use a new type here?
  final case class ListElement[To](parent: ProjectionExpression[_], index: Int) extends ProjectionExpression[To]

  /**
   * Unsafe version of `parse` that throws an exception rather than returning an Either
   * @see [[parse]]
   */
  def $(s: String): ProjectionExpression[Unknown] =
    parse(s) match {
      case Right(a)  => a.unsafeTo[Unknown]
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
   * @return either a `Right` of ProjectionExpression if successful, else a list of errors in a string
   */
  def parse(s: String): Either[String, ProjectionExpression[_]] = {

    final case class Builder(pe: Option[Either[Chunk[String], ProjectionExpression[_]]] = None) { self =>

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
        def multiDimPe(pe: ProjectionExpression[_], indexes: List[Int]): ProjectionExpression[_] =
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

      def either: Either[Chunk[String], ProjectionExpression[_]] =
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
