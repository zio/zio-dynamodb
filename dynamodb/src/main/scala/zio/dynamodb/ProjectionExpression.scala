package zio.dynamodb

import zio.Chunk
import zio.dynamodb.Annotations.{ maybeCaseName, maybeDiscriminator }
import zio.dynamodb.ConditionExpression.Operand.ProjectionExpressionOperand
import zio.dynamodb.ProjectionExpression.{ ListElement, MapElement, Root }
import zio.dynamodb.UpdateExpression.SetOperand.{ IfNotExists, ListAppend, ListPrepend, PathOperand }
import zio.dynamodb.proofs._
import zio.schema.{ AccessorBuilder, Schema }

import scala.annotation.tailrec
import proofs.Containable

// The maximum depth for a document path is 32
sealed trait ProjectionExpression[-From, +To] { self =>

  def >>>[To2](that: ProjectionExpression[To, To2]): ProjectionExpression[From, To2] =
    that match {
      case ProjectionExpression.Root                       =>
        self.asInstanceOf[ProjectionExpression[From, To2]]
      case ProjectionExpression.MapElement(parent, key)    =>
        ProjectionExpression
          .MapElement(self >>> parent, key)
      case ProjectionExpression.ListElement(parent, index) =>
        ProjectionExpression
          .ListElement(self >>> parent, index)
    }

  def elementAt[To2](
    index: Int
  )(implicit ev: To <:< Iterable[To2]): ProjectionExpression[From, To2] = {
    val _ = ev
    ProjectionExpression
      .ListElement(self, index)
      .asInstanceOf[ProjectionExpression[From, To2]]
  }

  /**
   * DDB keys must be strings
   */
  def valueAt[To2](key: String)(implicit ev: To <:< Map[String, To2]): ProjectionExpression[From, To2] = {
    val _ = ev
    ProjectionExpression
      .MapElement(self, key)
      .asInstanceOf[ProjectionExpression[From, To2]]
  }

  def unsafeTo[To2](implicit ev: To <:< ProjectionExpression.Unknown): ProjectionExpression[From, To2] = {
    val _ = ev
    self.asInstanceOf[ProjectionExpression[From, To2]]
  }

  def unsafeFrom[From2]: ProjectionExpression[From2, To] =
    self.asInstanceOf[ProjectionExpression[From2, To]]

  def apply(index: Int): ProjectionExpression[From, ProjectionExpression.Unknown] =
    ProjectionExpression.listElement(self, index)

  def apply(key: String): ProjectionExpression[From, ProjectionExpression.Unknown] =
    ProjectionExpression.mapElement(self, key)

  // unary ConditionExpressions

  // applies to all types
  def exists: ConditionExpression[From]    = ConditionExpression.AttributeExists(self)
  // applies to all types
  def notExists: ConditionExpression[From] = ConditionExpression.AttributeNotExists(self)
  // Applies to all types except Number and Boolean
  def size[To2 >: To](implicit ev: Sizable[To2]): ConditionExpression.Operand.Size[From, To2] = {
    val _ = ev
    ConditionExpression.Operand.Size(self, ev)
  }

  /**
   * Removes an element at the specified index from a list. Note that index is zero based
   */
  def remove[From2 <: From](
    index: Int
  )(implicit ev: ListRemoveable[To]): UpdateExpression.Action.RemoveAction[From2] = {
    val _ = ev
    UpdateExpression.Action.RemoveAction(ProjectionExpression.ListElement(self, index))
  }

  // apply to all types
  def isBinary: ConditionExpression[From]    = isType(AttributeValueType.Binary)
  def isNumber: ConditionExpression[From]    = isType(AttributeValueType.Number)
  def isString: ConditionExpression[From]    = isType(AttributeValueType.String)
  def isBool: ConditionExpression[From]      = isType(AttributeValueType.Bool)
  def isBinarySet: ConditionExpression[From] = isType(AttributeValueType.BinarySet)
  def isList: ConditionExpression[From]      = isType(AttributeValueType.List)
  def isMap: ConditionExpression[From]       = isType(AttributeValueType.Map)
  def isNumberSet: ConditionExpression[From] = isType(AttributeValueType.NumberSet)
  def isNull: ConditionExpression[From]      = isType(AttributeValueType.Null)
  def isStringSet: ConditionExpression[From] = isType(AttributeValueType.StringSet)

  /**
   * Only applies to a string attribute
   */
  def beginsWith(av: String)(implicit ev: Beginnable[String, To]): ConditionExpression[From] = {
    val _ = ev
    ConditionExpression.BeginsWith(self, AttributeValue.String(av))
  }

  // UpdateExpression conversions

  /**
   * Removes this PathExpression from an item
   */
  def remove[From2 <: From]: UpdateExpression.Action.RemoveAction[From2] =
    UpdateExpression.Action.RemoveAction[From2](self)

  override def toString: String = {
    @tailrec
    def loop(pe: ProjectionExpression[_, _], acc: List[String]): List[String] =
      pe match {
        case Root                                               =>
          acc // identity
        case ProjectionExpression.MapElement(Root, pathSegment) =>
          loop(Root, acc :+ pathSegment)
        case MapElement(parent, key)                            =>
          val pathSegment = key
          loop(parent, acc :+ "." + pathSegment)
        case ListElement(parent, index)                         =>
          loop(parent, acc :+ s"[$index]")
      }

    loop(self, List.empty).reverse.mkString
  }

  private def isType(attributeType: AttributeValueType): ConditionExpression[From] =
    ConditionExpression.AttributeType(self, attributeType)

}

trait ProjectionExpressionLowPriorityImplicits0 extends ProjectionExpressionLowPriorityImplicits1 {
  implicit class ProjectionExpressionSyntax0[From, To: ToAttributeValue](self: ProjectionExpression[From, To]) {

    def partitionKey(implicit ev: IsPrimaryKey[To]): PartitionKey[From, To] = {
      val _ = ev
      self match {
        case ProjectionExpression.MapElement(_, key) => PartitionKey[From, To](key)
        case _                                       => throw new IllegalArgumentException("Not a partition key") // should not happen
      }
    }
    def sortKey(implicit ev: IsPrimaryKey[To]): SortKey[From, To] = {
      val _ = ev
      self match {
        case ProjectionExpression.MapElement(_, key) => SortKey[From, To](key)
        case _                                       => throw new IllegalArgumentException("Not a partition key") // should not happen
      }
    }

    def set(a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self,
        UpdateExpression.SetOperand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(a))
      )

    def set(pe: ProjectionExpression[From, To]): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(self, PathOperand(pe))

    /**
     *  Set attribute if it does not exists
     */
    def setIfNotExists(a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(self, IfNotExists(self, implicitly[ToAttributeValue[To]].toAttributeValue(a)))

    /**
     * Append `a` to this list attribute
     */
    def append[A](
      a: A
    )(implicit ev: To <:< Iterable[A], to: ToAttributeValue[A]): UpdateExpression.Action.SetAction[From, To] =
      appendList(List(a).asInstanceOf[To])

    /**
     * Add list `xs` to the end of this list attribute
     */
    def appendList[A](
      xs: To
    )(implicit ev: To <:< Iterable[A], to: ToAttributeValue[A]): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self,
        ListAppend(self, AttributeValue.List(xs.toList.map(a => to.toAttributeValue(a))))
      )

    /**
     * Prepend `a` to this list attribute
     */
    def prepend[A](
      a: A
    )(implicit ev: To <:< Iterable[A], to: ToAttributeValue[A]): UpdateExpression.Action.SetAction[From, To] = {
      val _ = ev
      UpdateExpression.Action.SetAction(
        self,
        ListPrepend(self, AttributeValue.List(List(a).map(a => to.toAttributeValue(a))))
      )
    }

    /**
     * Add list `xs` to the beginning of this list attribute
     */
    def prependList[A](
      xs: To
    )(implicit ev: To <:< Iterable[A], to: ToAttributeValue[A]): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self,
        ListPrepend(self, AttributeValue.List(xs.toList.map(a => to.toAttributeValue(a))))
      )

    def between(minValue: To, maxValue: To): ConditionExpression[From] =
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .between(
          implicitly[ToAttributeValue[To]].toAttributeValue(minValue),
          implicitly[ToAttributeValue[To]].toAttributeValue(maxValue)
        )

    /**
     * Remove all elements of parameter `set` from this set attribute
     */
    def deleteFromSet(set: To)(implicit ev: To <:< Set[_]): UpdateExpression.Action.DeleteAction[From] = {
      val _ = ev
      UpdateExpression.Action.DeleteAction(self, implicitly[ToAttributeValue[To]].toAttributeValue(set))
    }

    def inSet(values: Set[To]): ConditionExpression[From] =
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .in(values.map(implicitly[ToAttributeValue[To]].toAttributeValue))

    def in(value: To, values: To*): ConditionExpression[From] = {
      val set: Set[To] = values.toSet + value
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .in(set.map(implicitly[ToAttributeValue[To]].toAttributeValue))
    }

    /**
     * Applies to a String or Set
     */
    def contains[A](av: A)(implicit ev: Containable[To, A], to: ToAttributeValue[A]): ConditionExpression[From] = {
      val _ = ev
      ConditionExpression.Contains(self, to.toAttributeValue(av))
    }

    /**
     * adds this value as a number attribute if it does not exists, else adds the numeric value to the existing attribute
     */
    def add(a: To)(implicit ev: Addable[To, To]): UpdateExpression.Action.AddAction[From] = {
      val _ = ev
      UpdateExpression.Action.AddAction(self, implicitly[ToAttributeValue[To]].toAttributeValue(a))
    }

    /**
     * Adds this set as an attribute if it does not exists, else if it exists it adds the elements of the set
     * Note that `A` must be a scalar type and is further restricted to Number, String, or Binary
     */
    def addSet[A](
      set: Set[A]
    )(implicit ev: Addable[To, A], evSet: Set[A] <:< To): UpdateExpression.Action.AddAction[From] = {
      val (_, _) = (ev, evSet)
      UpdateExpression.Action.AddAction(
        self,
        implicitly[ToAttributeValue[To]].toAttributeValue(evSet(set))
      )
    }

    def ===(that: To): ConditionExpression[From]                             =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def ===(that: ProjectionExpression[From, To]): ConditionExpression[From] =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def <>(that: To): ConditionExpression[From]                             =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def <>(that: ProjectionExpression[From, To]): ConditionExpression[From] =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def <(that: To): ConditionExpression[From]                             =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def <(that: ProjectionExpression[From, To]): ConditionExpression[From] =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def <=(that: To): ConditionExpression[From]                             =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def <=(that: ProjectionExpression[From, To]): ConditionExpression[From] =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def >(that: To): ConditionExpression[From]                             =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def >(that: ProjectionExpression[From, To]): ConditionExpression[From] =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def >=(that: To): ConditionExpression[From]                             =
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def >=(that: ProjectionExpression[From, To]): ConditionExpression[From] =
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

  }
}

trait ProjectionExpressionLowPriorityImplicits1 {
  implicit class ProjectionExpressionSyntax1[From, To](self: ProjectionExpression[From, To]) {

    /**
     * Modify or Add an item Attribute
     */
    def set(a: To)(implicit to: ToAttributeValue[To]): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self,
        UpdateExpression.SetOperand.ValueOperand(to.toAttributeValue(a))
      )

    def set[From1 <: From](that: ProjectionExpression[From1, To]): UpdateExpression.Action.SetAction[From1, To] =
      UpdateExpression.Action.SetAction(self, PathOperand(that))

    /**
     * Add item attribute if it does not exists
     */
    def setIfNotExists(a: To)(implicit to: ToAttributeValue[To]): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self,
        IfNotExists(self, to.toAttributeValue(a))
      )

    /**
     * Add item attribute if it does not exists
     */
    def setIfNotExists(
      that: ProjectionExpression[From, To], // TODO: To should be Unknown?
      a: To
    )(implicit
      to: ToAttributeValue[To]
    ): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action
        .SetAction(
          self,
          IfNotExists(that, to.toAttributeValue(a))
        )

    def append(a: To)(implicit to: ToAttributeValue[To]): UpdateExpression.Action.SetAction[From, To] =
      appendList(List(a))

    /**
     * Add list `xs` to the end of this list attribute
     */
    def appendList(xs: Iterable[To])(implicit to: ToAttributeValue[To]): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self,
        ListAppend(self, AttributeValue.List(xs.toList.map(a => to.toAttributeValue(a))))
      )

    /**
     * Prepend `a` to this list attribute
     */
    def prepend(a: To)(implicit to: ToAttributeValue[To]): UpdateExpression.Action.SetAction[From, To] =
      prependList(List(a))

    /**
     * Add list `xs` to the beginning of this list attribute
     */
    def prependList(xs: Iterable[To])(implicit to: ToAttributeValue[To]): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self,
        ListPrepend(self, AttributeValue.List(xs.toList.map(a => to.toAttributeValue(a))))
      )

    def between(minValue: To, maxValue: To)(implicit to: ToAttributeValue[To]): ConditionExpression[From] =
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .between(to.toAttributeValue(minValue), to.toAttributeValue(maxValue))

    /**
     * Remove all elements of parameter "set" from this set
     */
    def deleteFromSet[To2](
      set: To2
    )(implicit ev: To2 <:< Set[_], to: ToAttributeValue[To2]): UpdateExpression.Action.DeleteAction[From] = {
      val _ = ev
      UpdateExpression.Action.DeleteAction(self, to.toAttributeValue(set))
    }

    def inSet[To2](values: Set[To2])(implicit to: ToAttributeValue[To2]): ConditionExpression[From] =
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .in(values.map(to.toAttributeValue))
        .asInstanceOf[ConditionExpression[From]]

    def in[To2](value: To2, values: To2*)(implicit to: ToAttributeValue[To2]): ConditionExpression[From] = {
      val set: Set[To2] = values.toSet + value
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .in(set.map(to.toAttributeValue))
    }

    /**
     * Applies to a String or Set
     */
    def contains[To2](av: To2)(implicit to: ToAttributeValue[To2]): ConditionExpression[From] =
      ConditionExpression.Contains(self, to.toAttributeValue(av))

    /**
     * adds a number attribute if it does not exists, else adds the numeric value to the existing attribute
     */
    def add[To2](a: To2)(implicit to: ToAttributeValue[To2]): UpdateExpression.Action.AddAction[From] =
      UpdateExpression.Action.AddAction(self, to.toAttributeValue(a))

    /**
     * adds a set attribute if it does not exists, else if it exists it adds the elements of the set
     */
    def addSet[To2: ToAttributeValue](
      set: To2
    )(implicit ev: To2 <:< Set[_]): UpdateExpression.Action.AddAction[From] = {
      val _ = ev
      UpdateExpression.Action.AddAction(
        self,
        implicitly[ToAttributeValue[To2]].toAttributeValue(set)
      )
    }

    def ===[To2: ToAttributeValue](that: To2): ConditionExpression[From] =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To2]].toAttributeValue(that))
      )

    def ===[To2](
      that: ProjectionExpression[From, To2]
    )(implicit refersTo: RefersTo[To, To2]): ConditionExpression[From] = {
      val _ = refersTo
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )
    }

    // TODO: think about using != instead
    def <>[To2](
      that: ProjectionExpression[From, To2]
    )(implicit refersTo: RefersTo[To, To2]): ConditionExpression[From] = {
      val _ = refersTo
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )
    }
    def <[To2](
      that: ProjectionExpression[From, To2]
    )(implicit refersTo: RefersTo[To, To2]): ConditionExpression[From] = {
      val _ = refersTo
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )
    }
    def <=[To2](
      that: ProjectionExpression[From, To2]
    )(implicit refersTo: RefersTo[To, To2]): ConditionExpression[From] = {
      val _ = refersTo
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )
    }
    def >[To2](
      that: ProjectionExpression[From, To2]
    )(implicit refersTo: RefersTo[To, To2]): ConditionExpression[From] = {
      val _ = refersTo
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )
    }
    def >=[To2](
      that: ProjectionExpression[From, To2]
    )(implicit refersTo: RefersTo[To, To2]): ConditionExpression[From] = {
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

  def some[A]: ProjectionExpression[Option[A], A] =
    ProjectionExpression.root.asInstanceOf[ProjectionExpression[Option[A], A]]

  sealed trait OpticType
  object OpticType {
    case object Lens                                extends OpticType
    case class Prism(discriminator: Option[String]) extends OpticType
  }

  implicit class ProjectionExpressionSyntax[From](self: ProjectionExpression[From, Unknown]) {

    def partitionKey: PartitionKey[From, Unknown] =
      self match {
        case ProjectionExpression.MapElement(_, key) => PartitionKey[From, Unknown](key)
        case _                                       => throw new IllegalArgumentException("Not a partition key") // should not happen
      }
    def sortKey: SortKey[From, Unknown]           =
      self match {
        case ProjectionExpression.MapElement(_, key) => SortKey[From, Unknown](key)
        case _                                       => throw new IllegalArgumentException("Not a partition key") // should not happen
      }

    /**
     * Modify or Add an item Attribute
     */
    def set[To: ToAttributeValue](a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self.unsafeTo[To],
        UpdateExpression.SetOperand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(a))
      )

    /**
     * Modify or Add an item Attribute
     */
    def set[From1 <: From, To](that: ProjectionExpression[From1, To]): UpdateExpression.Action.SetAction[From1, To] =
      UpdateExpression.Action.SetAction(self.unsafeTo, PathOperand(that))

    /**
     * Add item attribute if it does not exists
     */
    def setIfNotExists[To: ToAttributeValue](a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self.unsafeTo,
        IfNotExists(self, implicitly[ToAttributeValue[To]].toAttributeValue(a))
      )

    /**
     * Add item attribute if it does not exists
     */
    def setIfNotExists[To: ToAttributeValue](
      that: ProjectionExpression[From, ProjectionExpression.Unknown],
      a: To
    ): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self.unsafeTo,
        IfNotExists(that, implicitly[ToAttributeValue[To]].toAttributeValue(a))
      )

    def append[A](a: A)(implicit to: ToAttributeValue[A]): UpdateExpression.Action.SetAction[From, A] =
      appendList(List(a))

    /**
     * Add list `xs` to the end of this list attribute
     */
    def appendList[To: ToAttributeValue](xs: Iterable[To]): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self.unsafeTo,
        ListAppend(self, AttributeValue.List(xs.toList.map(a => implicitly[ToAttributeValue[To]].toAttributeValue(a))))
      )

    /**
     * Prepend `a` to this list attribute
     */
    def prepend[To: ToAttributeValue](a: To): UpdateExpression.Action.SetAction[From, To] =
      prependList(List(a))

    /**
     * Add list `xs` to the beginning of this list attribute
     */
    def prependList[To: ToAttributeValue](xs: Iterable[To]): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self.unsafeTo,
        ListPrepend(self, AttributeValue.List(xs.toList.map(a => implicitly[ToAttributeValue[To]].toAttributeValue(a))))
      )

    def between[To](minValue: To, maxValue: To)(implicit to: ToAttributeValue[To]): ConditionExpression[From] =
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .between(to.toAttributeValue(minValue), to.toAttributeValue(maxValue))

    /**
     * Remove all elements of parameter "set" from this set
     */
    def deleteFromSet[To](
      set: To
    )(implicit ev: To <:< Set[_], to: ToAttributeValue[To]): UpdateExpression.Action.DeleteAction[From] = {
      val _ = ev
      UpdateExpression.Action.DeleteAction(self, to.toAttributeValue(set))
    }

    def inSet[To](values: Set[To])(implicit to: ToAttributeValue[To]): ConditionExpression[From] =
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .in(values.map(to.toAttributeValue))

    def in[To](value: To, values: To*)(implicit to: ToAttributeValue[To]): ConditionExpression[From] = {
      val set: Set[To] = values.toSet + value
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .in(set.map(to.toAttributeValue))
    }

    /**
     * Applies to a String or Set
     */
    def contains[To](av: To)(implicit to: ToAttributeValue[To]): ConditionExpression[From] =
      ConditionExpression.Contains(self, to.toAttributeValue(av))

    /**
     * adds a number attribute if it does not exists, else adds the numeric value to the existing attribute
     */
    def add[To](a: To)(implicit to: ToAttributeValue[To]): UpdateExpression.Action.AddAction[From] =
      UpdateExpression.Action.AddAction(self, to.toAttributeValue(a))

    /**
     * adds a set attribute if it does not exists, else if it exists it adds the elements of the set
     */
    def addSet[To: ToAttributeValue](set: To)(implicit ev: To <:< Set[_]): UpdateExpression.Action.AddAction[From] = {
      val _ = ev
      UpdateExpression.Action.AddAction(
        self,
        implicitly[ToAttributeValue[To]].toAttributeValue(set)
      )
    }

    def ===[To: ToAttributeValue](that: To): ConditionExpression[From] =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )

    def ===(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def <>[To: ToAttributeValue](that: To): ConditionExpression[From]        =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def <>(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def <[To: ToAttributeValue](that: To): ConditionExpression[From]        =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def <(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def <=[To: ToAttributeValue](that: To): ConditionExpression[From]        =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def <=(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def >[To: ToAttributeValue](that: To): ConditionExpression[From]        =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def >(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def >=[To: ToAttributeValue](that: To): ConditionExpression[From]        =
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(that))
      )
    def >=(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )
  }

  val builder = new AccessorBuilder {
    override type Lens[F, From, To]   = ProjectionExpression[From, To]
    override type Prism[F, From, To]  = ProjectionExpression[From, To]
    override type Traversal[From, To] = Unit

    // respects @caseName annotation
    override def makeLens[F, S, A](product: Schema.Record[S], term: Schema.Field[S, A]): Lens[F, S, A] = {

      val label = maybeCaseName(term.annotations).getOrElse(term.name)
      ProjectionExpression.MapElement(Root, label)
    }

    def makePrism[F, S, A](sum: Schema.Enum[S], term: Schema.Case[S, A]): Prism[F, S, A] =
      maybeDiscriminator(sum.annotations) match {
        case Some(_) =>
          ProjectionExpression.Root.asInstanceOf[Prism[F, S, A]]
        case None    =>
          ProjectionExpression
            .MapElement(Root, maybeCaseName(term.annotations).getOrElse(term.id))
            .asInstanceOf[Prism[F, S, A]]
      }

    override def makeTraversal[S, A](collection: Schema.Collection[S, A], element: Schema[A]): Traversal[S, A] = ()
  }

  // where should we put this?
  def accessors[A](implicit s: Schema[A]): s.Accessors[builder.Lens, builder.Prism, builder.Traversal] =
    s.makeAccessors(builder)

  /*
  \.
  Matches the dot character.

  (?=(?:[^][^])*(?![^]*))
  Uses a positive lookahead to assert that the dot is followed by an even number of backticks.
  This ensures that the dot is outside a character sequence enclosed in backticks.
   */
  val regexDotOutsideBackticks = """\.(?=(?:[^`]*`[^`]*`)*(?![^`]*`))""".r

  private val regexMapElement     = """(^[a-zA-Z0-9_-]+|^`[^`]+`)""".r
  private val regexIndexedElement = """(^[a-zA-Z0-9_-]+|^`[^`]+`)(\[[0-9]+])+""".r
  private val regexGroupedIndexes = """(\[([0-9]+)])""".r

  // Note that you can only use a ProjectionExpression if the first character is a-z or A-Z and the second character
  // (if present) is a-z, A-Z, or 0-9. Also key words are not allowed
  // If this is not the case then you must use the Expression Attribute Names facility to create an alias.
  // Attribute names containing a dot "." must also use the Expression Attribute Names
  def apply(name: String) = ProjectionExpression.MapElement(Root, name)

  private[dynamodb] case object Root extends ProjectionExpression[Any, Any] // [Any, Nothing] ?

  private[dynamodb] final case class MapElement[From, To](parent: ProjectionExpression[From, _], key: String)
      extends ProjectionExpression[From, To]

  private[dynamodb] final case class ListElement[From, To](parent: ProjectionExpression[From, _], index: Int)
      extends ProjectionExpression[From, To]

  private[dynamodb] def root: ProjectionExpression[Any, Any]                           = Root
  private[dynamodb] def mapElement[A](parent: ProjectionExpression[A, _], key: String) =
    MapElement[A, ProjectionExpression.Unknown](parent, key)

  private[dynamodb] def listElement[A](parent: ProjectionExpression[A, _], index: Int) =
    ListElement[A, ProjectionExpression.Unknown](parent, index)

  /**
   * Unsafe version of `parse` that throws an IllegalStateException rather than returning an Either.
   * Note all path elements are substituted automatically using the `ExpressionAttributeNames` facility.
   * The underscore "_" and hypen "-" are allowed in the name without any escaping.
   * Any other special characters must be escaped with backticks as the first and last characters.
   *
   * Examples:
   * {{{
   * $("foo")            // simple map element
   * $("foo.bar[9].baz") // array element with nested access
   * $("`foo.bar`")      // simple map element with a dot in the name
   * $("foo_bar")        // simple map element
   * $("foo-bar")        // simple map element
   * }}}
   *
   * @see [[parse]]
   */
  def $(s: String): ProjectionExpression[Any, Unknown] =
    parse(s) match {
      case Right(a)  => a.unsafeFrom[Any].unsafeTo[Unknown]
      case Left(msg) => throw new IllegalStateException(msg)
    }

  /**
   * Parses a string into an ProjectionExpression
   * Note all path elements are substituted automatically using the `ExpressionAttributeNames` facility.
   * The underscore "_" and hypen "-" are allowed in the name without any escaping.
   * Any other special characters must be escaped with backticks as the first and last characters.
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
  def parse(s: String): Either[String, ProjectionExpression[Unknown, Unknown]] = {

    // used to accumulate the Errors or if there are none the ProjectionExpression
    final case class Builder(pe: Option[Either[Chunk[String], ProjectionExpression[Unknown, Unknown]]] = None) { self =>

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
        def multiDimPe(
          pe: ProjectionExpression[ProjectionExpression.Unknown, ProjectionExpression.Unknown],
          indexes: List[Int]
        ): ProjectionExpression[ProjectionExpression.Unknown, ProjectionExpression.Unknown] =
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

      def either
        : Either[Chunk[String], ProjectionExpression[ProjectionExpression.Unknown, ProjectionExpression.Unknown]] =
        self.pe.getOrElse(Left(Chunk("error - at least one element must be specified")))
    }

    if (s == null)
      Left("error - input string is 'null'")
    else if (s.startsWith(".") || s.endsWith("."))
      Left(s"error - input string '$s' is invalid")
    else {

      val elements: List[String] = regexDotOutsideBackticks.split(s).toList

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

  def projectionsFromSchema[A: Schema]: Chunk[ProjectionExpression[_, _]] =
    implicitly[Schema[A]] match {
      case r: Schema.Record[A] =>
        println(s"XXXXXXXXXX record: $r")
        r.fields.map { f =>
          println(s"XXXXXXXXXX field: ${f.name}")
          ProjectionExpression.MapElement(Root, f.name)
        }
      case _                   =>
        println(s"XXXXXXXXXX empty projections")
        Chunk.empty
    }

}
