package zio.dynamodb

import zio.Chunk
import zio.dynamodb.ConditionExpression.Operand.{ ProjectionExpressionOperand, ToAv }
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

  // constraint: None - applies to all types
  def exists: ConditionExpression            = ConditionExpression.AttributeExists(self)
  // constraint: None - applies to all types
  def notExists: ConditionExpression         = ConditionExpression.AttributeNotExists(self)
  // constraint: Applies to ALL except Number and Boolean
  // Sizable typeclass for all types we support including Unknown
  def size: ConditionExpression.Operand.Size = // TODO: is it worth trying to restrict this to only compare with numeric types?
    ConditionExpression.Operand.Size(self)

  // constraint: ALL
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

  // ConditionExpression with AttributeValue's

  // constraint: String OR Set
  // create a typeclass called containable that only has String and Set and Unknown instances
//  def contains[A](av: A)(implicit t: ToAttributeValue[A]): ConditionExpression =
//    ConditionExpression.Contains(self, t.toAttributeValue(av))

  // constraint: String
  def beginsWith(av: String)(implicit ev: RefersTo[String, To]): ConditionExpression = {
    val _ = ev
    ConditionExpression.BeginsWith(self, AttributeValue.String(av))
  }

  // constraint: NONE -
  // we make this tighter by using the source type as the constraint for A
  // could be done by extension methods as well
//  def between[A](minValue: A, maxValue: A)(implicit t: ToAttributeValue[A]): ConditionExpression =
//    ConditionExpression.Operand
//      .ProjectionExpressionOperand(self)
//      .between(t.toAttributeValue(minValue), t.toAttributeValue(maxValue))

//  def in[A](values: Set[A])(implicit t: ToAttributeValue[A]): ConditionExpression       =
//    ConditionExpression.Operand.ProjectionExpressionOperand(self).in(values.map(t.toAttributeValue))
//  def in[A](value: A, values: A*)(implicit t: ToAttributeValue[A]): ConditionExpression =
//    ConditionExpression.Operand
//      .ProjectionExpressionOperand(self)
//      .in(values.map(t.toAttributeValue).toSet + t.toAttributeValue(value))

  // UpdateExpression conversions

  // maybe we can derive AV from schema
  // can we provide schema for AV -
  // this would reduce API by half
  /*
  implicit val x: Schema[AttributeValue] = ??? // eg use DynamicValue.transform(f1, f2)
   */
//  implicit val x: Schema[AttributeValue] = ???
//  def foo[A: ToAttributeValue](a: A): Schema[AttributeValue] = ???

//  /**
//   * Modify or Add an item Attribute
//   */
//  def setValue[A](a: A)(implicit t: ToAttributeValue[A]): UpdateExpression.Action.SetAction =
//    UpdateExpression.Action.SetAction(self, UpdateExpression.SetOperand.ValueOperand(t.toAttributeValue(a)))

//  // we could restrict A to be same type as field
//  // have to move as extension method
//  def set[A: Schema](a: A): UpdateExpression.Action.SetAction = setValue(AttributeValue.encode(a))

//  /**
//   * Modify or Add an item Attribute
//   */
//  def set(pe: ProjectionExpression[_]): UpdateExpression.Action.SetAction =
//    UpdateExpression.Action.SetAction(self, PathOperand(pe))

//  /**
//   * Modifying or Add item Attributes if ProjectionExpression `pe` exists
//   */
//  // TODO: add Schema variant and move
//  def setIfNotExists[A](pe: ProjectionExpression[_], a: A)(implicit
//    t: ToAttributeValue[A]
//  ): UpdateExpression.Action.SetAction =
//    UpdateExpression.Action.SetAction(self, IfNotExists(pe, t.toAttributeValue(a)))

//  def setIfNotExists[A](a: A)(implicit t: ToAttributeValue[A]): UpdateExpression.Action.SetAction =
//    UpdateExpression.Action.SetAction(self, IfNotExists(self, t.toAttributeValue(a)))

  /**
   * Add list `xs` to the end of this PathExpression
   */
//  // TODO: add schema variant
//  def appendList[A](xs: Iterable[A])(implicit t: ToAttributeValue[A]): UpdateExpression.Action.SetAction =
//    UpdateExpression.Action.SetAction(self, ListAppend(self, AttributeValue.List(xs.map(t.toAttributeValue))))

//  /**
//   * Add list `xs` to the beginning of this PathExpression
//   */
//  // TODO: add schema variant
//  def prependList[A](xs: Iterable[A])(implicit t: ToAttributeValue[A]): UpdateExpression.Action.SetAction =
//    UpdateExpression.Action.SetAction(self, ListPrepend(self, AttributeValue.List(xs.map(t.toAttributeValue))))

  /**
   * Updating Numbers and Sets
   */
  // TODO: add schema variant
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
  // TODO: add schema variant
//  def deleteFromSet[A](a: A)(implicit t: ToAttributeValue[A]): UpdateExpression.Action.DeleteAction =
//    UpdateExpression.Action.DeleteAction(self, t.toAttributeValue(a))

  override def toString: String = {
    @tailrec
    def loop(pe: ProjectionExpression[_], acc: List[String]): List[String] =
      pe match {
        /*
        If you have a PE that DDB does not know how to handle, then you have an error
        eg [0] // DDB does not support top level array or primitives at the top level
        so we need more code everywhere it is used to check that ROOT is valid and maybe
        special case it when in the context of the top level.
         */
        case Root                                        => acc // identity
        case ProjectionExpression.MapElement(Root, name) => acc :+ s"$name"
        case MapElement(parent, key)                     => loop(parent, acc :+ s".$key")
        case ListElement(parent, index)                  => loop(parent, acc :+ s"[$index]")
      }

    loop(self, List.empty).reverse.mkString("")
  }
}

@implicitNotFound("the type ${A} must be a ${X} in order to use this operator")
sealed trait Containable[X, -A]
trait ContainableLowPriorityImplicits0 extends ContainableLowPriorityImplicits1 {
  implicit def unknownRight[X]: Containable[X, ProjectionExpression.Unknown] =
    new Containable[X, ProjectionExpression.Unknown] {}
}
trait ContainableLowPriorityImplicits1 {
  implicit def set[Set[A], A]: Containable[Set[A], A]      = new Containable[Set[A], A] {}
  implicit def string[String]: Containable[String, String] = new Containable[String, String] {}
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

  implicit class ProjectionExpressionSyntax0[To: ToAv](self: ProjectionExpression[To]) {
    def set(a: To): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(self, UpdateExpression.SetOperand.ValueOperand(implicitly[ToAv[To]].toAv(a)))

    def set(pe: ProjectionExpression[To]): UpdateExpression.Action.SetAction = {
      println(s"XXXXXXXXXXXX set 1")
      UpdateExpression.Action.SetAction(self, PathOperand(pe))
    }

    def setIfNotExists(a: To): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(self, IfNotExists(self, implicitly[ToAv[To]].toAv(a)))

    def appendList[A](xs: To)(implicit ev: To <:< Iterable[A], to: ToAv[A]): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(
        self,
        ListAppend(self, AttributeValue.List(xs.map(a => to.toAv(a))))
      )

    def prepend[A](a: A)(implicit ev: To <:< Iterable[A], to: ToAv[A]): UpdateExpression.Action.SetAction =
      prependList(List(a).asInstanceOf[To])

    def prependList[A](xs: To)(implicit ev: To <:< Iterable[A], to: ToAv[A]): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(self, ListPrepend(self, AttributeValue.List(xs.map(a => to.toAv(a)))))

    def between(minValue: To, maxValue: To): ConditionExpression =
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .between(implicitly[ToAv[To]].toAv(minValue), implicitly[ToAv[To]].toAv(maxValue))

    /*
    TODO: Avi fix the AWS interpreter for this - we get the following error
    [error] ║  ║  ║ software.amazon.awssdk.services.dynamodb.model.DynamoDbException: Invalid UpdateExpression: Incorrect operand
    type for operator or function; operator: DELETE, operand type: STRING, typeSet: ALLOWED_FOR_DELETE_OPERAND
    (Service: DynamoDb, Status Code: 400, Request ID: 4839352c-fe7f-4771-bb89-6d69a48ee935, Extended Request ID: null)
    TODO: Avi also add a test for this in LiveSpec
     */
    def deleteFromSet[A](a: A)(implicit ev: To <:< Set[A], to: ToAv[A]): UpdateExpression.Action.DeleteAction =
      UpdateExpression.Action.DeleteAction(self, to.toAv(a))

    def in[A](values: To)(implicit ev: To <:< Set[A], to: ToAv[A]): ConditionExpression =
      ConditionExpression.Operand.ProjectionExpressionOperand(self).in(values.map(to.toAv))

    def in[A](value: A, values: A*)(implicit ev: To <:< Set[A], to: ToAv[A]): ConditionExpression = {
      val set: Set[A] = values.toSet + value
      ConditionExpression.Operand.ProjectionExpressionOperand(self).in(set.map(to.toAv))
    }

    def contains[A](av: A)(implicit ev: Containable[To, A], to: ToAv[A]): ConditionExpression = {
      val _ = ev
      println(s"contains for Optics")
      ConditionExpression.Contains(self, to.toAv(av))
    }

    def ===(that: To): ConditionExpression =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAv[To]].toAv(that))
      )
//    def ===[To2](that: ProjectionExpression[To2])(implicit refersTo: RefersTo[To, To2]): ConditionExpression = {
//      val _ = refersTo
//      ConditionExpression.Equals(
//        ProjectionExpressionOperand(self),
//        ConditionExpression.Operand.ProjectionExpressionOperand(that)
//      )
//    }

    def <>(that: To): ConditionExpression =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAv[To]].toAv(that))
      )
//    def <>[To2](that: ProjectionExpression[To2])(implicit refersTo: RefersTo[To, To2]): ConditionExpression = {
//      val _ = refersTo
//      ConditionExpression.NotEqual(
//        ProjectionExpressionOperand(self),
//        ConditionExpression.Operand.ProjectionExpressionOperand(that)
//      )
//    }

    def <(that: To): ConditionExpression =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAv[To]].toAv(that))
      )
//    def <[To2](that: ProjectionExpression[To2])(implicit refersTo: RefersTo[To, To2]): ConditionExpression = {
//      val _ = refersTo
//      ConditionExpression.LessThan(
//        ProjectionExpressionOperand(self),
//        ConditionExpression.Operand.ProjectionExpressionOperand(that)
//      )
//    }

    def <=(that: To): ConditionExpression =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAv[To]].toAv(that))
      )
//    def <=[To2](that: ProjectionExpression[To2])(implicit refersTo: RefersTo[To, To2]): ConditionExpression = {
//      val _ = refersTo
//      ConditionExpression.LessThanOrEqual(
//        ProjectionExpressionOperand(self),
//        ConditionExpression.Operand.ProjectionExpressionOperand(that)
//      )
//    }

    def >(that: To): ConditionExpression =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAv[To]].toAv(that))
      )
//    def >[To2](that: ProjectionExpression[To2])(implicit refersTo: RefersTo[To, To2]): ConditionExpression = {
//      val _ = refersTo
//      ConditionExpression.GreaterThan(
//        ProjectionExpressionOperand(self),
//        ConditionExpression.Operand.ProjectionExpressionOperand(that)
//      )
//    }

    def >=(that: To): ConditionExpression =
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAv[To]].toAv(that))
      )
//    def >=[To2](that: ProjectionExpression[To2])(implicit refersTo: RefersTo[To, To2]): ConditionExpression = {
//      val _ = refersTo
//      ConditionExpression.GreaterThanOrEqual(
//        ProjectionExpressionOperand(self),
//        ConditionExpression.Operand.ProjectionExpressionOperand(that)
//      )
//    }

  }
}
trait ProjectionExpressionLowPriorityImplicits1 {
  implicit class ProjectionExpressionSyntax1[To](self: ProjectionExpression[To]) {
    def set[To2](
      that: ProjectionExpression[To]
    )(implicit refersTo: RefersTo[To, To2]): UpdateExpression.Action.SetAction = {
      val _ = refersTo
      println(s"XXXXXXXXXXXX set 2")
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
    def set[To: ToAv](a: To): UpdateExpression.Action.SetAction = {
      println(s"XXXXXXXXXXXX set 3")
      UpdateExpression.Action.SetAction(self, UpdateExpression.SetOperand.ValueOperand(implicitly[ToAv[To]].toAv(a)))
    }
    def set(that: ProjectionExpression[_]): UpdateExpression.Action.SetAction = {
      println(s"XXXXXXXXXXXX set 4")
      UpdateExpression.Action.SetAction(self, PathOperand(that))
    }

    def setIfNotExists[To: ToAv](a: To): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(self, IfNotExists(self, implicitly[ToAv[To]].toAv(a)))

    def setIfNotExists[To: ToAv](that: ProjectionExpression[_], a: To): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(self, IfNotExists(that, implicitly[ToAv[To]].toAv(a)))

    def appendList[A: ToAv](xs: Iterable[A]): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(
        self,
        ListAppend(self, AttributeValue.List(xs.map(a => implicitly[ToAv[A]].toAv(a))))
      )

    def prepend[A: ToAv](a: A): UpdateExpression.Action.SetAction =
      prependList(List(a))

    def prependList[A: ToAv](xs: Iterable[A]): UpdateExpression.Action.SetAction =
      UpdateExpression.Action.SetAction(
        self,
        ListPrepend(self, AttributeValue.List(xs.map(a => implicitly[ToAv[A]].toAv(a))))
      )

    def between[A](minValue: A, maxValue: A)(implicit to: ToAv[A]): ConditionExpression =
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .between(to.toAv(minValue), to.toAv(maxValue))

    def deleteFromSet[A](a: A)(implicit to: ToAv[A]): UpdateExpression.Action.DeleteAction =
      UpdateExpression.Action.DeleteAction(self, to.toAv(a))

    def in[A](values: Set[A])(implicit to: ToAv[A]): ConditionExpression =
      ConditionExpression.Operand.ProjectionExpressionOperand(self).in(values.map(to.toAv))

    def in[A](value: A, values: A*)(implicit to: ToAv[A]): ConditionExpression = {
      val set: Set[A] = values.toSet + value
      ConditionExpression.Operand.ProjectionExpressionOperand(self).in(set.map(to.toAv))
    }

    def contains[A](av: A)(implicit to: ToAv[A]): ConditionExpression = {
      println(s"contains for Unknown")
      ConditionExpression.Contains(self, to.toAv(av))
    }

    def ===[To: ToAv](that: To): ConditionExpression =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAv[To]].toAv(that))
      )

    def ===(that: ProjectionExpression[_]): ConditionExpression =
      ConditionExpression.Equals(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def <>[To: ToAv](that: To): ConditionExpression            =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAv[To]].toAv(that))
      )
    def <>(that: ProjectionExpression[_]): ConditionExpression =
      ConditionExpression.NotEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def <[To: ToAv](that: To): ConditionExpression            =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAv[To]].toAv(that))
      )
    def <(that: ProjectionExpression[_]): ConditionExpression =
      ConditionExpression.LessThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def <=[To: ToAv](that: To): ConditionExpression            =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAv[To]].toAv(that))
      )
    def <=(that: ProjectionExpression[_]): ConditionExpression =
      ConditionExpression.LessThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def >[To: ToAv](that: To): ConditionExpression            =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAv[To]].toAv(that))
      )
    def >(that: ProjectionExpression[_]): ConditionExpression =
      ConditionExpression.GreaterThan(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ProjectionExpressionOperand(that)
      )

    def >=[To: ToAv](that: To): ConditionExpression            =
      ConditionExpression.GreaterThanOrEqual(
        ProjectionExpressionOperand(self),
        ConditionExpression.Operand.ValueOperand(implicitly[ToAv[To]].toAv(that))
      )
    def >=(that: ProjectionExpression[_]): ConditionExpression =
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
