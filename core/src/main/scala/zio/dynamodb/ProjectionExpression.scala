/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.dynamodb

import zio.dynamodb.ConditionExpression.Operand.ProjectionExpressionOperand
import zio.dynamodb.UpdateExpression.SetOperand.{ IfNotExists, ListAppend, ListPrepend, PathOperand }

import scala.annotation.tailrec

/**
 * A path into a DynamoDB item attribute — the Low-Level API's counterpart to a
 * `CompanionOptics`-generated `Lens`. Built by chaining `.apply(key: String)`/
 * `.apply(index: Int)` off [[ProjectionExpression.Root]] (or more conveniently via the
 * companion object's `$` constructor, e.g. `$("orders").apply(0).apply("total")`), and
 * rendered as a dotted/bracketed path (`orders[0].total`) by `toString`.
 *
 * `From`/`To` are phantom type parameters carrying no runtime value — they exist purely to
 * let comparison/update operators (defined on the `ProjectionExpressionSyntax` implicit
 * class in the companion object) type-check against the item shape the expression is meant
 * to apply to.
 */
sealed trait ProjectionExpression[-From, +To] { self =>

  def unsafeTo[To2](implicit ev: To <:< ProjectionExpression.Unknown): ProjectionExpression[From, To2] = {
    val _ = ev
    self.asInstanceOf[ProjectionExpression[From, To2]]
  }

  def unsafeFrom[From2]: ProjectionExpression[From2, To] =
    self.asInstanceOf[ProjectionExpression[From2, To]]

  def >>>[To2](that: ProjectionExpression[To, To2]): ProjectionExpression[From, To2] =
    that match {
      case ProjectionExpression.Root                       =>
        self.asInstanceOf[ProjectionExpression[From, To2]]
      case ProjectionExpression.MapElement(parent, key)    =>
        ProjectionExpression.MapElement(self >>> parent, key)
      case ProjectionExpression.ListElement(parent, index) =>
        ProjectionExpression.ListElement(self >>> parent, index)
    }

  def apply(index: Int): ProjectionExpression[From, ProjectionExpression.Unknown] =
    ProjectionExpression.ListElement(self, index)

  def apply(key: String): ProjectionExpression[From, ProjectionExpression.Unknown] =
    ProjectionExpression.MapElement(self, key)

  override def toString: String = {
    @tailrec
    def loop(pe: ProjectionExpression[_, _], acc: List[String]): List[String] =
      pe match {
        case ProjectionExpression.Root                                           => acc
        case ProjectionExpression.MapElement(ProjectionExpression.Root, segment) =>
          loop(ProjectionExpression.Root, acc :+ segment)
        case ProjectionExpression.MapElement(parent, key)                        => loop(parent, acc :+ s".$key")
        case ProjectionExpression.ListElement(parent, index)                     => loop(parent, acc :+ s"[$index]")
      }
    loop(self, Nil).reverse.mkString
  }
}

/**
 * Constructors ([[ProjectionExpression.Root]], the `$` string-path builder) and the
 * `ProjectionExpressionSyntax` implicit class, which adds comparison operators (`===`, `<`,
 * `between`, `contains`, ...) building [[ConditionExpression]]s, and mutation operators
 * (`.set`, `.add`, `.appendList`, `.deleteFromSet`, ...) building [[UpdateExpression.Action]]s.
 */
object ProjectionExpression {

  type Unknown

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
        UpdateExpression.SetOperand.ValueOperand(ToAttributeValue[To].toAttributeValue(a))
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
        IfNotExists(self, ToAttributeValue[To].toAttributeValue(a))
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
        IfNotExists(that, ToAttributeValue[To].toAttributeValue(a))
      )

    def append[A](a: A)(implicit to: ToAttributeValue[A]): UpdateExpression.Action.SetAction[From, A] =
      appendList(List(a))

    /**
     * Add list `xs` to the end of this list attribute
     */
    def appendList[To: ToAttributeValue](xs: Iterable[To]): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self.unsafeTo,
        ListAppend(self, AttributeValue.List(xs.toList.map(a => ToAttributeValue[To].toAttributeValue(a))))
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
        ListPrepend(self, AttributeValue.List(xs.toList.map(a => ToAttributeValue[To].toAttributeValue(a))))
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
     * Applies fields of type Set, List, String and creates a composite of `contains` ConditionExpression's
     * for each element (head plus tail) that are joined with an `&&` (and)
     */
    def containsSet[To](headAv: To, tail: Set[To])(implicit to: ToAttributeValue[To]): ConditionExpression[From] =
      tail.foldLeft(contains(headAv))((acc, a) => acc && contains(a))

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
        ToAttributeValue[To].toAttributeValue(set)
      )
    }

    def ===[To: ToAttributeValue](that: To): ConditionExpression[From] =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )

    def ===(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def <>[To: ToAttributeValue](that: To): ConditionExpression[From]        =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )
    def <>(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def <[To: ToAttributeValue](that: To): ConditionExpression[From]        =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )
    def <(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def <=[To: ToAttributeValue](that: To): ConditionExpression[From]        =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )
    def <=(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def >[To: ToAttributeValue](that: To): ConditionExpression[From]        =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )
    def >(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def >=[To: ToAttributeValue](that: To): ConditionExpression[From]        =
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )
    def >=(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )
  }

  private[dynamodb] case object Root extends ProjectionExpression[Any, Any]

  private[dynamodb] final case class MapElement[From, To](
    parent: ProjectionExpression[From, _],
    key: String
  ) extends ProjectionExpression[From, To]

  private[dynamodb] final case class ListElement[From, To](
    parent: ProjectionExpression[From, _],
    index: Int
  ) extends ProjectionExpression[From, To]

  private[dynamodb] def mapElement[A](
    parent: ProjectionExpression[A, _],
    key: String
  ): ProjectionExpression[A, Unknown] =
    MapElement[A, Unknown](parent, key)

  private[dynamodb] def listElement[A](
    parent: ProjectionExpression[A, _],
    index: Int
  ): ProjectionExpression[A, Unknown] =
    ListElement[A, Unknown](parent, index)

  def $(s: String): ProjectionExpression[Any, Unknown] = ProjectionExpressionParser.$(s)
}
