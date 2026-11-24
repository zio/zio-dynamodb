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
/**
 * A path to an attribute of a DynamoDB item, such as `address.city` or `tags[0]`. `From` is the type of the item the
 * path starts from and `To` is the type of the attribute it reaches. Paths name the attributes that a query reads
 * (projections), and their operators build a [[ConditionExpression]] (`===`, `>`, `exists`, `beginsWith`, ...) or an
 * [[UpdateExpression]] action (`set`, `remove`, `add`, ...).
 *
 * There are two ways to get one. `ProjectionExpression.accessors[A]` derives a typed path for each field of a case class
 * from its `Schema`, so the operators only accept values of the field's type. The `\$` function parses a string into a
 * path whose `To` type is `ProjectionExpression.Unknown`, so the operators accept a value of any type that has a
 * [[ToAttributeValue]].
 *
 * {{{
 * import zio.dynamodb.ProjectionExpression.\$
 * import zio.schema.{ DeriveSchema, Schema }
 *
 * final case class Address(city: String, zip: String)
 * object Address {
 *   implicit val schema: Schema.CaseClass2[String, String, Address] = DeriveSchema.gen[Address]
 *   val (city, zip) = ProjectionExpression.accessors[Address]
 * }
 *
 * final case class Customer(id: String, address: Address, tags: List[String])
 * object Customer {
 *   implicit val schema: Schema.CaseClass3[String, Address, List[String], Customer] = DeriveSchema.gen[Customer]
 *   val (id, address, tags) = ProjectionExpression.accessors[Customer]
 * }
 *
 * val cityPath: ProjectionExpression[Customer, String] = Customer.address >>> Address.city
 * cityPath.toString                  // address.city
 * Customer.tags.elementAt(0).toString // tags[0]
 *
 * val inParis: ConditionExpression[Customer] = cityPath === "Paris" && Customer.tags.size > 1
 * val untyped: ConditionExpression[Any]      = \$("address.city") === "Paris"
 * }}}
 *
 * @see [[https://zio.dev/zio-dynamodb/reference/projection-expression/ ProjectionExpression]]
 */
sealed trait ProjectionExpression[-From, +To] { self =>

  /**
   * Appends the path `that`, which starts from this path's attribute, to the end of this path. For example
   * `Customer.address >>> Address.city` is the path `address.city`.
   */
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

  /**
   * Returns the path to the element at `index` of this list attribute, such as `tags[2]`. The index is zero based.
   */
  def elementAt[To2](
    index: Int
  )(implicit ev: To <:< Iterable[To2]): ProjectionExpression[From, To2] = {
    val _ = ev
    ProjectionExpression
      .ListElement(self, index)
      .asInstanceOf[ProjectionExpression[From, To2]]
  }

  /**
   * Returns the path to the value under `key` of this map attribute, such as `meta.colour`. DynamoDB map keys are
   * strings, so the attribute must be a `Map[String, To2]`.
   */
  def valueAt[To2](key: String)(implicit ev: To <:< Map[String, To2]): ProjectionExpression[From, To2] = {
    val _ = ev
    ProjectionExpression
      .MapElement(self, key)
      .asInstanceOf[ProjectionExpression[From, To2]]
  }

  /**
   * Returns this path with its attribute type changed from `ProjectionExpression.Unknown` to `To2`. Nothing is checked:
   * it is a cast.
   */
  def unsafeTo[To2](implicit ev: To <:< ProjectionExpression.Unknown): ProjectionExpression[From, To2] = {
    val _ = ev
    self.asInstanceOf[ProjectionExpression[From, To2]]
  }

  /** Returns this path with its item type changed to `From2`. Nothing is checked: it is a cast. */
  def unsafeFrom[From2]: ProjectionExpression[From2, To] =
    self.asInstanceOf[ProjectionExpression[From2, To]]

  /**
   * Returns the path to the list element at `index` of this attribute, such as `tags[3]`. The index is zero based and
   * the result's attribute type is `ProjectionExpression.Unknown`.
   */
  def apply(index: Int): ProjectionExpression[From, ProjectionExpression.Unknown] =
    ProjectionExpression.listElement(self, index)

  /**
   * Returns the path to the map key or nested attribute `key` of this attribute, such as `address.city`. The result's
   * attribute type is `ProjectionExpression.Unknown`.
   */
  def apply(key: String): ProjectionExpression[From, ProjectionExpression.Unknown] =
    ProjectionExpression.mapElement(self, key)

  // unary ConditionExpressions

  /** Builds the condition `attribute_exists(path)`, which holds when the item has this attribute. */
  def exists: ConditionExpression[From] = ConditionExpression.AttributeExists(self)

  /** Builds the condition `attribute_not_exists(path)`, which holds when the item does not have this attribute. */
  def notExists: ConditionExpression[From] = ConditionExpression.AttributeNotExists(self)

  /**
   * Returns the operand `size(path)`, the size of this attribute, to compare with a number: `Customer.tags.size > 1`.
   * It compiles for attributes of type `String`, any `Iterable` (lists, sets, maps) and `ProjectionExpression.Unknown`.
   */
  def size[To2 >: To](implicit ev: Sizable[To2]): ConditionExpression.Operand.Size[From, To2] = {
    val _ = ev
    ConditionExpression.Operand.Size(self, ev)
  }

  /**
   * Builds the update action `REMOVE path[index]`, which removes the element at `index` from this list attribute. The
   * index is zero based. It compiles for attributes whose type is a `Seq` or `ProjectionExpression.Unknown`.
   */
  def remove[From2 <: From](
    index: Int
  )(implicit ev: ListRemoveable[To]): UpdateExpression.Action.RemoveAction[From2] = {
    val _ = ev
    UpdateExpression.Action.RemoveAction(ProjectionExpression.ListElement(self, index))
  }

  /** Builds the condition `attribute_type(path, B)`, which holds when this attribute is binary. */
  def isBinary: ConditionExpression[From] = isType(AttributeValueType.Binary)

  /** Builds the condition `attribute_type(path, N)`, which holds when this attribute is a number. */
  def isNumber: ConditionExpression[From] = isType(AttributeValueType.Number)

  /** Builds the condition `attribute_type(path, S)`, which holds when this attribute is a string. */
  def isString: ConditionExpression[From] = isType(AttributeValueType.String)

  /** Builds the condition `attribute_type(path, BOOL)`, which holds when this attribute is a boolean. */
  def isBool: ConditionExpression[From] = isType(AttributeValueType.Bool)

  /** Builds the condition `attribute_type(path, BS)`, which holds when this attribute is a binary set. */
  def isBinarySet: ConditionExpression[From] = isType(AttributeValueType.BinarySet)

  /** Builds the condition `attribute_type(path, L)`, which holds when this attribute is a list. */
  def isList: ConditionExpression[From] = isType(AttributeValueType.List)

  /** Builds the condition `attribute_type(path, M)`, which holds when this attribute is a map. */
  def isMap: ConditionExpression[From] = isType(AttributeValueType.Map)

  /** Builds the condition `attribute_type(path, NS)`, which holds when this attribute is a number set. */
  def isNumberSet: ConditionExpression[From] = isType(AttributeValueType.NumberSet)

  /** Builds the condition `attribute_type(path, NULL)`, which holds when this attribute is null. */
  def isNull: ConditionExpression[From] = isType(AttributeValueType.Null)

  /** Builds the condition `attribute_type(path, SS)`, which holds when this attribute is a string set. */
  def isStringSet: ConditionExpression[From] = isType(AttributeValueType.StringSet)

  /**
   * Builds the condition `begins_with(path, av)`, which holds when this attribute starts with the string `av`. It
   * compiles for attributes of type `String`, `Option[String]` and `ProjectionExpression.Unknown`.
   */
  def beginsWith(av: String)(implicit ev: Beginnable[String, To]): ConditionExpression[From] = {
    val _ = ev
    ConditionExpression.BeginsWith(self, AttributeValue.String(av))
  }

  // UpdateExpression conversions

  /** Builds the update action `REMOVE path`, which removes this attribute from the item. */
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

/**
 * The operators of a typed [[ProjectionExpression]], one whose attribute type `To` is known (for example a path from
 * `ProjectionExpression.accessors`). Values passed to them must be of type `To`.
 */
trait ProjectionExpressionLowPriorityImplicits {

  /**
   * Adds the condition, update and key operators to a typed path. The value arguments must be of the attribute's type
   * `To`, so `Customer.age > "x"` does not compile when `age` is an `Int`.
   */
  implicit class ProjectionExpressionSyntax2[From, To: ToAttributeValue](self: ProjectionExpression[From, To]) {

    /**
     * Returns this path as the partition key of a table or index, to build a key condition with `===`. The key name is
     * the last map key of the path, and `To` must be a string, number or binary type, or an `Option` of one.
     *
     * @throws java.lang.IllegalArgumentException
     *   when the path ends with a list index, such as `tags[0]`
     */
    def partitionKey(implicit ev: IsPrimaryKey[To]): PartitionKey[From, To] = {
      val _ = ev
      self match {
        case ProjectionExpression.MapElement(_, key) => PartitionKey[From, To](key)
        case _                                       => throw new IllegalArgumentException("Not a partition key") // should not happen
      }
    }

    /**
     * Returns this path as the sort key of a table or index, to build a key condition such as `===`, `>` or
     * `beginsWith`. The key name is the last map key of the path, and `To` must be a string, number or binary type, or
     * an `Option` of one.
     *
     * @throws java.lang.IllegalArgumentException
     *   when the path ends with a list index, such as `tags[0]` (the message says "Not a partition key")
     */
    def sortKey(implicit ev: IsPrimaryKey[To]): SortKey[From, To] = {
      val _ = ev
      self match {
        case ProjectionExpression.MapElement(_, key) => SortKey[From, To](key)
        case _                                       => throw new IllegalArgumentException("Not a partition key") // should not happen
      }
    }

    /** Builds the update action `SET path = a`, which adds this attribute or replaces its value with `a`. */
    def set(a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self,
        UpdateExpression.SetOperand.ValueOperand(ToAttributeValue[To].toAttributeValue(a))
      )

    /**
     * Builds the update action `SET path = pe`, which sets this attribute to the current value of the attribute at
     * `pe`.
     */
    def set(pe: ProjectionExpression[From, To]): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(self, PathOperand(pe))

    /**
     * Builds the update action `SET path = if_not_exists(path, a)`, which sets this attribute to `a` only when the item
     * does not have it yet.
     */
    def setIfNotExists(a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(self, IfNotExists(self, ToAttributeValue[To].toAttributeValue(a)))

    /**
     * Builds the update action `SET path = list_append(path, [a])`, which appends `a` to the end of this list
     * attribute.
     */
    def append[A](
      a: A
    )(implicit ev: To <:< Iterable[A], to: ToAttributeValue[A]): UpdateExpression.Action.SetAction[From, To] =
      appendList(List(a).asInstanceOf[To])

    /**
     * Builds the update action `SET path = list_append(path, xs)`, which appends the elements of `xs` to the end of
     * this list attribute.
     */
    def appendList[A](
      xs: To
    )(implicit ev: To <:< Iterable[A], to: ToAttributeValue[A]): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self,
        ListAppend(self, AttributeValue.List(xs.toList.map(a => to.toAttributeValue(a))))
      )

    /**
     * Builds the update action `SET path = list_append([a], path)`, which adds `a` to the beginning of this list
     * attribute.
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
     * Builds the update action `SET path = list_append(xs, path)`, which adds the elements of `xs` to the beginning of
     * this list attribute.
     */
    def prependList[A](
      xs: To
    )(implicit ev: To <:< Iterable[A], to: ToAttributeValue[A]): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self,
        ListPrepend(self, AttributeValue.List(xs.toList.map(a => to.toAttributeValue(a))))
      )

    /** Builds the condition `path BETWEEN minValue AND maxValue`. */
    def between(minValue: To, maxValue: To): ConditionExpression[From] =
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .between(
          ToAttributeValue[To].toAttributeValue(minValue),
          ToAttributeValue[To].toAttributeValue(maxValue)
        )

    /**
     * Builds the update action `DELETE path set`, which removes the elements of `set` from this set attribute.
     */
    def deleteFromSet(set: To)(implicit ev: To <:< Set[_]): UpdateExpression.Action.DeleteAction[From] = {
      val _ = ev
      UpdateExpression.Action.DeleteAction(self, ToAttributeValue[To].toAttributeValue(set))
    }

    /** Builds the condition `path IN (v1, v2, ...)`, which holds when this attribute equals one of `values`. */
    def inSet(values: Set[To]): ConditionExpression[From] =
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .in(values.map(ToAttributeValue[To].toAttributeValue))

    /**
     * Builds the condition `path IN (value, values...)`, which holds when this attribute equals one of the arguments.
     * The arguments are put in a `Set`, so duplicates are dropped and their order is not kept.
     */
    def in(value: To, values: To*): ConditionExpression[From] = {
      val set: Set[To] = values.toSet + value
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .in(set.map(ToAttributeValue[To].toAttributeValue))
    }

    /**
     * Builds the condition `contains(path, a)`: the string attribute contains the substring `a`, or the set or list
     * attribute contains the element `a`. It compiles for attributes of type `Set[A]`, `List[A]`, `String` and
     * `Option[String]`.
     */
    def contains[A](a: A)(implicit ev: Containable[To, A], to: ToAttributeValue[A]): ConditionExpression[From] = {
      val _ = ev
      ConditionExpression.Contains(self, to.toAttributeValue(a))
    }

    /**
     * Builds one `contains` condition for `head` and for each element of `tail`, joined with `&&`, so it holds when the
     * attribute contains all of them. It compiles for the same attribute types as `contains`: `Set[A]`, `List[A]`,
     * `String` and `Option[String]`.
     */
    def containsSet[A](head: A, tail: Set[A])(implicit
      ev: Containable[To, A],
      to: ToAttributeValue[A]
    ): ConditionExpression[From] =
      tail.foldLeft(contains(head))((acc, a) => acc && contains(a))

    /**
     * Builds the update action `ADD path a`, which adds the number `a` to this number attribute, or sets the attribute
     * to `a` when the item does not have it yet. It compiles for attributes of type `Int`, `Long`, `Short`, `Float` and
     * `Double`.
     */
    def add(a: To)(implicit ev: Addable[To, To]): UpdateExpression.Action.AddAction[From] = {
      val _ = ev
      UpdateExpression.Action.AddAction(self, ToAttributeValue[To].toAttributeValue(a))
    }

    /**
     * Builds the update action `ADD path set`, which adds the elements of `set` to this set attribute, or sets the
     * attribute to `set` when the item does not have it yet. DynamoDB sets hold strings, numbers or binary values.
     */
    def addSet[A](
      set: Set[A]
    )(implicit ev: Addable[To, A], evSet: Set[A] <:< To): UpdateExpression.Action.AddAction[From] = {
      val (_, _) = (ev, evSet)
      UpdateExpression.Action.AddAction(
        self,
        ToAttributeValue[To].toAttributeValue(evSet(set))
      )
    }

    /** Builds the condition `path = that`, which compares this attribute with the value `that`. */
    def ===(that: To): ConditionExpression[From] =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )

    /** Builds the condition `path = that`, which compares this attribute with the attribute at `that`. */
    def ===(that: ProjectionExpression[From, To]): ConditionExpression[From] =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    /** Builds the condition `path <> that`, which compares this attribute with the value `that`. */
    def <>(that: To): ConditionExpression[From] =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )

    /** Builds the condition `path <> that`, which compares this attribute with the attribute at `that`. */
    def <>(that: ProjectionExpression[From, To]): ConditionExpression[From] =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    /** Builds the condition `path < that`, which compares this attribute with the value `that`. */
    def <(that: To): ConditionExpression[From] =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )

    /** Builds the condition `path < that`, which compares this attribute with the attribute at `that`. */
    def <(that: ProjectionExpression[From, To]): ConditionExpression[From] =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    /** Builds the condition `path <= that`, which compares this attribute with the value `that`. */
    def <=(that: To): ConditionExpression[From] =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )

    /** Builds the condition `path <= that`, which compares this attribute with the attribute at `that`. */
    def <=(that: ProjectionExpression[From, To]): ConditionExpression[From] =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    /** Builds the condition `path > that`, which compares this attribute with the value `that`. */
    def >(that: To): ConditionExpression[From] =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )

    /** Builds the condition `path > that`, which compares this attribute with the attribute at `that`. */
    def >(that: ProjectionExpression[From, To]): ConditionExpression[From] =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    /** Builds the condition `path >= that`, which compares this attribute with the value `that`. */
    def >=(that: To): ConditionExpression[From] =
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )

    /** Builds the condition `path >= that`, which compares this attribute with the attribute at `that`. */
    def >=(that: ProjectionExpression[From, To]): ConditionExpression[From] =
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

  }
}

/**
 * Builds [[ProjectionExpression]] values: `\$` and `parse` read a path from a string, and `accessors` derives one typed
 * path per field of a case class from its `Schema`. It also holds the operators of untyped paths
 * (`ProjectionExpressionSyntax`).
 */
object ProjectionExpression extends ProjectionExpressionLowPriorityImplicits {

  /**
   * The attribute type of a path whose type is not known, such as one from `\$("name")`. The operators of such a path
   * accept a value of any type that has a [[ToAttributeValue]].
   */
  type Unknown

  /**
   * Returns a path that steps from an `Option[A]` into the `A` inside it. It adds no path element, so
   * `Person.address >>> ProjectionExpression.some >>> Address.city` is the path `address.city`.
   */
  def some[A]: ProjectionExpression[Option[A], A] =
    ProjectionExpression.root.asInstanceOf[ProjectionExpression[Option[A], A]]

  /**
   * The kind of optic an accessor stands for: a `Lens` for a field of a record, or a `Prism` for a case of a sum type.
   * No method of the library takes or returns it.
   */
  sealed trait OpticType

  /** The two kinds of [[ProjectionExpression.OpticType]]. */
  object OpticType {

    /** The optic of a field of a record. */
    case object Lens extends OpticType

    /** The optic of a case of a sum type, with the name of its discriminator field when the sum type has one. */
    case class Prism(discriminator: Option[String]) extends OpticType
  }

  /**
   * Adds the condition, update and key operators to an untyped path, one whose attribute type is
   * `ProjectionExpression.Unknown`, such as `\$("age")`. Each operator accepts a value of any type that has a
   * [[ToAttributeValue]], so the compiler does not check that it matches the attribute.
   */
  implicit class ProjectionExpressionSyntax[From](self: ProjectionExpression[From, Unknown]) {

    /**
     * Returns this path as the partition key of a table or index, to build a key condition with `===`. The key name is
     * the last map key of the path: `\$("id").partitionKey` names the key `id`.
     *
     * @throws java.lang.IllegalArgumentException when the path ends with a list index, such as `\$("tags[0]")`
     */
    def partitionKey: PartitionKey[From, Unknown] =
      self match {
        case ProjectionExpression.MapElement(_, key) => PartitionKey[From, Unknown](key)
        case _                                       => throw new IllegalArgumentException("Not a partition key") // should not happen
      }

    /**
     * Returns this path as the sort key of a table or index, to build a key condition such as `===`, `>` or
     * `beginsWith`. The key name is the last map key of the path.
     *
     * @throws java.lang.IllegalArgumentException when the path ends with a list index, such as `\$("tags[0]")` (the
     *   message says "Not a partition key")
     */
    def sortKey: SortKey[From, Unknown] =
      self match {
        case ProjectionExpression.MapElement(_, key) => SortKey[From, Unknown](key)
        case _                                       => throw new IllegalArgumentException("Not a partition key") // should not happen
      }

    /** Builds the update action `SET path = a`, which adds this attribute or replaces its value with `a`. */
    def set[To: ToAttributeValue](a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self.unsafeTo[To],
        UpdateExpression.SetOperand.ValueOperand(ToAttributeValue[To].toAttributeValue(a))
      )

    /**
     * Builds the update action `SET path = that`, which sets this attribute to the current value of the attribute at
     * `that`.
     */
    def set[From1 <: From, To](that: ProjectionExpression[From1, To]): UpdateExpression.Action.SetAction[From1, To] =
      UpdateExpression.Action.SetAction(self.unsafeTo, PathOperand(that))

    /**
     * Builds the update action `SET path = if_not_exists(path, a)`, which sets this attribute to `a` only when the item
     * does not have it yet.
     */
    def setIfNotExists[To: ToAttributeValue](a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self.unsafeTo,
        IfNotExists(self, ToAttributeValue[To].toAttributeValue(a))
      )

    /**
     * Builds the update action `SET path = if_not_exists(that, a)`, which sets this attribute to the value of the
     * attribute at `that`, or to `a` when the item does not have `that`.
     */
    def setIfNotExists[To: ToAttributeValue](
      that: ProjectionExpression[From, ProjectionExpression.Unknown],
      a: To
    ): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self.unsafeTo,
        IfNotExists(that, ToAttributeValue[To].toAttributeValue(a))
      )

    /**
     * Builds the update action `SET path = list_append(path, [a])`, which appends `a` to the end of this list
     * attribute.
     */
    def append[A](a: A)(implicit to: ToAttributeValue[A]): UpdateExpression.Action.SetAction[From, A] =
      appendList(List(a))

    /**
     * Builds the update action `SET path = list_append(path, xs)`, which appends the elements of `xs` to the end of
     * this list attribute.
     */
    def appendList[To: ToAttributeValue](xs: Iterable[To]): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self.unsafeTo,
        ListAppend(self, AttributeValue.List(xs.toList.map(a => ToAttributeValue[To].toAttributeValue(a))))
      )

    /**
     * Builds the update action `SET path = list_append([a], path)`, which adds `a` to the beginning of this list
     * attribute.
     */
    def prepend[To: ToAttributeValue](a: To): UpdateExpression.Action.SetAction[From, To] =
      prependList(List(a))

    /**
     * Builds the update action `SET path = list_append(xs, path)`, which adds the elements of `xs` to the beginning of
     * this list attribute.
     */
    def prependList[To: ToAttributeValue](xs: Iterable[To]): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self.unsafeTo,
        ListPrepend(self, AttributeValue.List(xs.toList.map(a => ToAttributeValue[To].toAttributeValue(a))))
      )

    /** Builds the condition `path BETWEEN minValue AND maxValue`. */
    def between[To](minValue: To, maxValue: To)(implicit to: ToAttributeValue[To]): ConditionExpression[From] =
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .between(to.toAttributeValue(minValue), to.toAttributeValue(maxValue))

    /**
     * Builds the update action `DELETE path set`, which removes the elements of `set` from this set attribute.
     */
    def deleteFromSet[To](
      set: To
    )(implicit ev: To <:< Set[_], to: ToAttributeValue[To]): UpdateExpression.Action.DeleteAction[From] = {
      val _ = ev
      UpdateExpression.Action.DeleteAction(self, to.toAttributeValue(set))
    }

    /** Builds the condition `path IN (v1, v2, ...)`, which holds when this attribute equals one of `values`. */
    def inSet[To](values: Set[To])(implicit to: ToAttributeValue[To]): ConditionExpression[From] =
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .in(values.map(to.toAttributeValue))

    /**
     * Builds the condition `path IN (value, values...)`, which holds when this attribute equals one of the arguments.
     * The arguments are put in a `Set`, so duplicates are dropped and their order is not kept.
     */
    def in[To](value: To, values: To*)(implicit to: ToAttributeValue[To]): ConditionExpression[From] = {
      val set: Set[To] = values.toSet + value
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .in(set.map(to.toAttributeValue))
    }

    /**
     * Builds the condition `contains(path, av)`: the string attribute contains the substring `av`, or the set or list
     * attribute contains the element `av`.
     */
    def contains[To](av: To)(implicit to: ToAttributeValue[To]): ConditionExpression[From] =
      ConditionExpression.Contains(self, to.toAttributeValue(av))

    /**
     * Builds one `contains` condition for `headAv` and for each element of `tail`, joined with `&&`, so it holds when
     * the attribute contains all of them.
     */
    def containsSet[To](headAv: To, tail: Set[To])(implicit to: ToAttributeValue[To]): ConditionExpression[From] =
      tail.foldLeft(contains(headAv))((acc, a) => acc && contains(a))

    /**
     * Builds the update action `ADD path a`. For a number `a` it adds `a` to this number attribute; for a set `a` it
     * adds the elements of `a` to this set attribute. When the item does not have the attribute yet, it is set to `a`.
     */
    def add[To](a: To)(implicit to: ToAttributeValue[To]): UpdateExpression.Action.AddAction[From] =
      UpdateExpression.Action.AddAction(self, to.toAttributeValue(a))

    /**
     * Builds the update action `ADD path set`, which adds the elements of `set` to this set attribute, or sets the
     * attribute to `set` when the item does not have it yet.
     */
    def addSet[To: ToAttributeValue](set: To)(implicit ev: To <:< Set[_]): UpdateExpression.Action.AddAction[From] = {
      val _ = ev
      UpdateExpression.Action.AddAction(
        self,
        ToAttributeValue[To].toAttributeValue(set)
      )
    }

    /** Builds the condition `path = that`, which compares this attribute with the value `that`. */
    def ===[To: ToAttributeValue](that: To): ConditionExpression[From] =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )

    /** Builds the condition `path = that`, which compares this attribute with the attribute at `that`. */
    def ===(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    /** Builds the condition `path <> that`, which compares this attribute with the value `that`. */
    def <>[To: ToAttributeValue](that: To): ConditionExpression[From] =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )

    /** Builds the condition `path <> that`, which compares this attribute with the attribute at `that`. */
    def <>(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    /** Builds the condition `path < that`, which compares this attribute with the value `that`. */
    def <[To: ToAttributeValue](that: To): ConditionExpression[From] =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )

    /** Builds the condition `path < that`, which compares this attribute with the attribute at `that`. */
    def <(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    /** Builds the condition `path <= that`, which compares this attribute with the value `that`. */
    def <=[To: ToAttributeValue](that: To): ConditionExpression[From] =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )

    /** Builds the condition `path <= that`, which compares this attribute with the attribute at `that`. */
    def <=(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    /** Builds the condition `path > that`, which compares this attribute with the value `that`. */
    def >[To: ToAttributeValue](that: To): ConditionExpression[From] =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )

    /** Builds the condition `path > that`, which compares this attribute with the attribute at `that`. */
    def >(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    /** Builds the condition `path >= that`, which compares this attribute with the value `that`. */
    def >=[To: ToAttributeValue](that: To): ConditionExpression[From] =
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(ToAttributeValue[To].toAttributeValue(that))
      )

    /** Builds the condition `path >= that`, which compares this attribute with the attribute at `that`. */
    def >=(that: ProjectionExpression[From, Any]): ConditionExpression[From] =
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )
  }

  /**
   * The zio-schema `AccessorBuilder` that `accessors` uses: each lens and prism it makes is a [[ProjectionExpression]].
   * A lens is the path to a field, named by the field's `@caseName` annotation when it has one. A prism is the path to
   * the case's name (its `@caseName` or its id), or an empty path when the sum type has a `@discriminatorName`
   * annotation. Traversals are `Unit`.
   */
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
  /**
   * Derives the typed paths of `A` from its `Schema`: one [[ProjectionExpression]] per field of a case class (as a
   * tuple when there are several), or one per case of a sealed trait. Each path starts from `A` and reaches the field's
   * type, so the operators only accept values of that type.
   *
   * {{{
   * import zio.schema.{ DeriveSchema, Schema }
   *
   * final case class Book(isbn: String, title: String, pages: Int)
   * object Book {
   *   implicit val schema: Schema.CaseClass3[String, String, Int, Book] = DeriveSchema.gen[Book]
   *   val (isbn, title, pages) = ProjectionExpression.accessors[Book]
   * }
   *
   * val p: ProjectionExpression[Book, Int] = Book.pages
   * val long: ConditionExpression[Book]    = Book.pages > 300
   * val retitle                            = Book.title.set("Dune")
   * }}}
   */
  def accessors[A](implicit s: Schema[A]): s.Accessors[builder.Lens, builder.Prism, builder.Traversal] =
    s.makeAccessors(builder)

  /*
  \.
  Matches the dot character.

  (?=(?:[^][^])*(?![^]*))
  Uses a positive lookahead to assert that the dot is followed by an even number of backticks.
  This ensures that the dot is outside a character sequence enclosed in backticks.
   */
  private val regexDotOutsideBackticks = """\.(?=(?:[^`]*`[^`]*`)*(?![^`]*`))""".r

  private val regexMapElement     = """(^[a-zA-Z0-9_-]+|^`[^`]+`)""".r
  private val regexIndexedElement = """(^[a-zA-Z0-9_-]+|^`[^`]+`)(\[[0-9]+])+""".r
  private val regexGroupedIndexes = """(\[([0-9]+)])""".r

  // Note that you can only use a ProjectionExpression if the first character is a-z or A-Z and the second character
  // (if present) is a-z, A-Z, or 0-9. Also key words are not allowed
  // If this is not the case then you must use the Expression Attribute Names facility to create an alias.
  // Attribute names containing a dot "." must also use the Expression Attribute Names
  /**
   * Returns the path to the top level attribute `name`. The name is not parsed, so `ProjectionExpression("a.b")` is one
   * attribute whose name contains a dot; use `\$` to parse a nested path.
   */
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
   * Parses `s` into an untyped path, like `parse`, but throws instead of returning a `Left`. Use it for paths written
   * in the code, where a parse error is a programming mistake.
   *
   * Path elements are separated by dots, and a list index is written `[n]`. Letters, digits, the underscore "_" and the
   * hyphen "-" need no escaping. A name with any other character, such as a dot, must start and end with a backtick.
   * Each name is sent to DynamoDB as an expression attribute name (`#n0`, `#n1`, ...), so reserved words need no
   * escaping. Currently the backticks are removed from the name only when it is the first element of the path; for
   * a nested element they stay in the attribute name sent to DynamoDB.
   *
   * {{{
   * import zio.dynamodb.ProjectionExpression.\$
   *
   * val email  = \$("email")              // a top level attribute
   * val sku    = \$("order.lines[0].sku") // nested map and list elements
   * val dotted = \$("`first.name`")       // one attribute whose name contains a dot
   * val under  = \$("ship_to")            // underscores need no backticks
   * val hyphen = \$("ship-to")            // nor do hyphens
   *
   * sku.toString // order.lines[0].sku
   * }}}
   *
   * @throws java.lang.IllegalStateException when `s` is not a valid path, with the message that `parse` returns
   * @see [[parse]]
   * @see [[https://zio.dev/zio-dynamodb/reference/low-level-api/dollar-function/ The \$ Function]]
   */
  def $(s: String): ProjectionExpression[Any, Unknown] =
    $$(s)

  /**
   * Parses `s` into a path with the item type `From` and the attribute type `To` that the caller chooses, and throws
   * when `s` is not a valid path. Nothing checks that the types match the item: they are casts. It accepts the same
   * syntax as `\$`.
   *
   * @throws java.lang.IllegalStateException when `s` is not a valid path, with the message that `parse` returns
   */
  def $$[From, To](s: String): ProjectionExpression[From, To] =
    parse(s) match {
      case Right(a)  => a.unsafeFrom[From].unsafeTo[To]
      case Left(msg) => throw new IllegalStateException(msg)
    }

  /**
   * Parses `s` into an untyped path. Path elements are separated by dots, and a list index is written `[n]` (several
   * indexes, as in `grid[1][2]`, are allowed). Letters, digits, the underscore "_" and the hyphen "-" need no escaping;
   * a name with any other character must start and end with a backtick. Each name is sent to DynamoDB as an expression
   * attribute name, so reserved words need no escaping.
   *
   * {{{
   * import zio.dynamodb.ProjectionExpression.parse
   *
   * val ok      = parse("order.lines[0].sku").map(_.toString) // Right(order.lines[0].sku)
   * val spaces  = parse("first name.last name")               // Left(error with 'first name',error with 'last name')
   * val trailer = parse("order.")                             // Left(error - input string 'order.' is invalid)
   * }}}
   *
   * @param s
   *   the path, such as `order.lines[0].sku`
   * @return
   *   the path, or a `Left` with the errors separated by commas: one `error with 'e'` for each element `e` that is
   *   not valid, `error - input string 's' is invalid` when `s` starts or ends with a dot, and
   *   `error - input string is 'null'` when `s` is `null`
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

      val builder = elements.foldLeft(Builder()) { case (accBuilder, s) =>
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

  /**
   * Returns one top level path per field of `A` when its schema is a record (a case class), named by the field names of
   * the schema, and an empty `Chunk` for any other schema. `DynamoDBQuery.get` uses it to read only the fields of `A`.
   */
  def projectionsFromSchema[A: Schema]: Chunk[ProjectionExpression[_, _]] =
    Schema[A] match {
      case r: Schema.Record[A] =>
        r.fields.map { f =>
          ProjectionExpression.MapElement(Root, f.name)
        }
      case _                   => Chunk.empty
    }

}
