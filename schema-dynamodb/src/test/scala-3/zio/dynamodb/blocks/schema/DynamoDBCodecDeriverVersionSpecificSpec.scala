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

package zio.dynamodb.blocks.schema

import zio.blocks.schema.{ Modifier, Schema }
import zio.blocks.schema.json.DiscriminatorKind
import zio.dynamodb.AttributeValue
import zio.dynamodb.blocks.schema.DynamoDBCodecDeriver
import zio.test._
import zio.test.Assertion.{ contains, equalTo, hasField, hasKey, isSome, isSubtype }

object DynamoDBCodecDeriverVersionSpecificSpec extends ZIOSpecDefault {

  // ---------- Scala 3 enum models ------------------------------------------

  enum Direction derives Schema {
    case North, South, East, West
  }

  @Modifier.caseNaming("snake_case")
  enum CardinalDirection derives Schema {
    case NorthEast, SouthWest, NorthWest, SouthEast
  }

  enum Shape derives Schema {
    case Circle(radius: Int)
    case Rect(width: Int, height: Int)
  }

  @Modifier.discriminator("kind")
  enum TaggedShape derives Schema {
    case Circle(radius: Int)
    case Rect(width: Int, height: Int)
  }

  @Modifier.caseNaming("snake_case")
  enum SnakeShape derives Schema {
    case CircleShape(radius: Int)
    case RectShape(w: Int, h: Int)
  }

  enum Event derives Schema {
    @Modifier.rename("mouse_click")
    case Click(x: Int, y: Int)
    case KeyPress(code: Int)
  }

  // Parameterised recursive enum: covariant so Leaf (Tree[Nothing]) is usable
  // as Tree[T] at any concrete T.
  enum Tree[+T] {
    case Branch(value: T, left: Tree[T], right: Tree[T])
    case Leaf
  }

  // Multi-level sealed hierarchy with only constant leaf cases
  sealed trait Animal derives Schema
  case object Cat     extends Animal
  sealed trait BigCat extends Animal
  case object Lion    extends BigCat
  case object Tiger   extends BigCat

  // Recursive parameterised enum with @Modifier.rename on both case and fields
  enum Chain[+T] {
    @Modifier.rename("cons")
    case Cons(
      @Modifier.rename("hd") head: T,
      @Modifier.rename("tl") tail: Chain[T]
    )
    case Nil
  }

  // ---------- Scala 3 opaque types -----------------------------------------

  opaque type InvoiceId = String
  object InvoiceId:
    given Schema[InvoiceId]         = Schema.string
    def apply(s: String): InvoiceId = s

  opaque type Quantity = Int
  object Quantity:
    given Schema[Quantity]      = Schema.int
    def apply(n: Int): Quantity = n

  case class LineItem(invoiceId: InvoiceId, qty: Quantity) derives Schema

  // IArray wrapper: custom equals/hashCode required because IArray is array-backed
  case class IArrayRecord(xs: IArray[String]) {
    override def hashCode(): Int           =
      java.util.Arrays.hashCode(xs.asInstanceOf[Array[AnyRef]])
    override def equals(obj: Any): Boolean = obj match {
      case that: IArrayRecord =>
        java.util.Arrays.equals(xs.asInstanceOf[Array[AnyRef]], that.xs.asInstanceOf[Array[AnyRef]])
      case _                  => false
    }
  }

  // ---------- helpers -------------------------------------------------------

  private given treeIntSchema: Schema[Tree[Int]]         = Schema.derived
  private given treeStringSchema: Schema[Tree[String]]   = Schema.derived
  private given chainIntSchema: Schema[Chain[Int]]       = Schema.derived
  private given chainDoubleSchema: Schema[Chain[Double]] = Schema.derived
  private given chainStringSchema: Schema[Chain[String]] = Schema.derived
  private given iArrayRecordSchema: Schema[IArrayRecord] = Schema.derived

  private def codecFor[A](implicit s: Schema[A]): DynamoDBCodec[A] =
    s.deriving(DynamoDBCodecDeriver).derive

  private def roundTrip[A: Schema](value: A): Either[?, A] = {
    val c = codecFor[A]
    c.decoder(c.encoder(value))
  }

  // ---------- spec ----------------------------------------------------------

  private val dvCodec = DynamoDBCodecDeriver.dynamicValueCodec

  def spec = suite("DynamoDBCodecDeriverVersionSpecificSpec")(
    constantValueEnumSuite,
    constantValueEnumSnakeCaseSuite,
    dataCarryingEnumKeySuite,
    dataCarryingEnumFieldSuite,
    dataCarryingEnumSnakeCaseSuite,
    enumRenameCase,
    parameterisedEnumSuite,
    multiLevelHierarchySuite,
    unionTypeKeyDiscriminatorSuite,
    unionTypeNoneDiscriminatorSuite,
    genericTupleSuite,
    nestedGenericRecordSuite,
    iArraySuite,
    renamedRecursiveEnumSuite,
    recursiveADTNoneDiscriminatorSuite,
    nestedVariantsNoneDiscriminatorSuite,
    opaqueTypeSuite,
    dynamicValueVariantSuite
  )

  // ---- constant-value Scala 3 enum ----------------------------------------

  private val constantValueEnumSuite =
    suite("constant-value Scala 3 enum")(
      test("each case encodes as AttributeValue.String of its name") {
        val codec = codecFor[Direction]
        assertTrue(
          codec.encoder(Direction.North) == AttributeValue.String("North") &&
            codec.encoder(Direction.South) == AttributeValue.String("South") &&
            codec.encoder(Direction.East) == AttributeValue.String("East") &&
            codec.encoder(Direction.West) == AttributeValue.String("West")
        )
      },
      test("decodes AttributeValue.String back to the matching case") {
        val codec = codecFor[Direction]
        assertTrue(
          codec.decoder(AttributeValue.String("North")) == Right(Direction.North) &&
            codec.decoder(AttributeValue.String("West")) == Right(Direction.West)
        )
      },
      test("round-trips all cases") {
        val codec = codecFor[Direction]
        val all   = List(Direction.North, Direction.South, Direction.East, Direction.West)
        assertTrue(all.forall(d => codec.decoder(codec.encoder(d)) == Right(d)))
      },
      test("decode error for unknown case name") {
        val codec = codecFor[Direction]
        assertTrue(codec.decoder(AttributeValue.String("Up")).isLeft)
      },
      test("decode error when AttributeValue is not a String") {
        val codec = codecFor[Direction]
        assertTrue(codec.decoder(AttributeValue.Number(BigDecimal(1))).isLeft)
      }
    )

  // ---- constant-value Scala 3 enum with @Modifier.caseNaming snake_case ---

  private val constantValueEnumSnakeCaseSuite =
    suite("constant-value Scala 3 enum with @Modifier.caseNaming snake_case")(
      test("cases encode as snake_case AttributeValue.String values") {
        val codec = codecFor[CardinalDirection]
        assertTrue(
          codec.encoder(CardinalDirection.NorthEast) == AttributeValue.String("north_east") &&
            codec.encoder(CardinalDirection.SouthWest) == AttributeValue.String("south_west") &&
            codec.encoder(CardinalDirection.NorthWest) == AttributeValue.String("north_west") &&
            codec.encoder(CardinalDirection.SouthEast) == AttributeValue.String("south_east")
        )
      },
      test("round-trips all snake_case cases") {
        val codec = codecFor[CardinalDirection]
        val all   = List(
          CardinalDirection.NorthEast,
          CardinalDirection.SouthWest,
          CardinalDirection.NorthWest,
          CardinalDirection.SouthEast
        )
        assertTrue(all.forall(d => codec.decoder(codec.encoder(d)) == Right(d)))
      },
      test("decode error when original PascalCase name is used") {
        val codec = codecFor[CardinalDirection]
        assertTrue(codec.decoder(AttributeValue.String("NorthEast")).isLeft)
      }
    )

  // ---- data-carrying Scala 3 enum with Key discriminator ------------------

  private val dataCarryingEnumKeySuite =
    suite("data-carrying Scala 3 enum with Key discriminator")(
      test("Circle encodes as singleton Map with case name as key") {
        val codec = codecFor[Shape]
        assert(codec.encoder(Shape.Circle(5)))(
          isSubtype[AttributeValue.Map](
            hasField("value", _.value.keys.map(_.value).toList, equalTo(List("Circle")))
          )
        )
      },
      test("round-trips Circle") {
        assertTrue(roundTrip[Shape](Shape.Circle(5)) == Right(Shape.Circle(5)))
      },
      test("round-trips Rect") {
        assertTrue(roundTrip[Shape](Shape.Rect(10, 20)) == Right(Shape.Rect(10, 20)))
      },
      test("decode error for unknown case key") {
        val codec = codecFor[Shape]
        val badAv = AttributeValue.Map(
          Map(AttributeValue.String("Triangle") -> AttributeValue.Map(Map.empty))
        )
        assertTrue(codec.decoder(badAv).isLeft)
      }
    )

  // ---- data-carrying Scala 3 enum with Field discriminator ----------------

  private val dataCarryingEnumFieldSuite =
    suite("data-carrying Scala 3 enum with @Modifier.discriminator (Field encoding)")(
      test("Circle encodes as flat Map with 'kind' discriminator field") {
        val codec = codecFor[TaggedShape]
        assert(codec.encoder(TaggedShape.Circle(7)))(
          isSubtype[AttributeValue.Map](
            hasField(
              "value",
              _.value.keys.map(_.value).toSet,
              contains("kind") && contains("radius") && !contains("Circle")
            )
          )
        )
      },
      test("'kind' discriminator field value is the case name") {
        val codec = codecFor[TaggedShape]
        assert(codec.encoder(TaggedShape.Circle(7)))(
          isSubtype[AttributeValue.Map](
            hasField(
              "value",
              _.value.get(AttributeValue.String("kind")),
              isSome(equalTo(AttributeValue.String("Circle")))
            )
          )
        )
      },
      test("round-trips Circle") {
        assertTrue(roundTrip[TaggedShape](TaggedShape.Circle(7)) == Right(TaggedShape.Circle(7)))
      },
      test("round-trips Rect") {
        assertTrue(roundTrip[TaggedShape](TaggedShape.Rect(3, 4)) == Right(TaggedShape.Rect(3, 4)))
      },
      test("decode error when 'kind' field is absent") {
        val codec = codecFor[TaggedShape]
        val av    = AttributeValue.Map(Map(AttributeValue.String("radius") -> AttributeValue.Number(5)))
        assertTrue(codec.decoder(av).isLeft)
      }
    )

  // ---- data-carrying Scala 3 enum with snake_case case names --------------

  private val dataCarryingEnumSnakeCaseSuite =
    suite("data-carrying Scala 3 enum with @Modifier.caseNaming snake_case")(
      test("CircleShape encodes under 'circle_shape' key") {
        val codec = codecFor[SnakeShape]
        assert(codec.encoder(SnakeShape.CircleShape(9)))(
          isSubtype[AttributeValue.Map](
            hasField("value", _.value.keys.map(_.value).toSet, equalTo(Set("circle_shape")))
          )
        )
      },
      test("round-trips CircleShape") {
        assertTrue(roundTrip[SnakeShape](SnakeShape.CircleShape(9)) == Right(SnakeShape.CircleShape(9)))
      },
      test("round-trips RectShape") {
        assertTrue(roundTrip[SnakeShape](SnakeShape.RectShape(3, 4)) == Right(SnakeShape.RectShape(3, 4)))
      },
      test("decode error when original PascalCase key is used") {
        val codec = codecFor[SnakeShape]
        val av    = AttributeValue.Map(
          Map(
            AttributeValue.String("CircleShape") -> AttributeValue.Map(
              Map(AttributeValue.String("radius") -> AttributeValue.Number(9))
            )
          )
        )
        assertTrue(codec.decoder(av).isLeft)
      }
    )

  // ---- Scala 3 enum with @Modifier.rename on a case ----------------------

  private val enumRenameCase =
    suite("Scala 3 enum with @Modifier.rename on a case")(
      test("renamed case uses the given wire name as Map key") {
        val codec = codecFor[Event]
        assert(codec.encoder(Event.Click(1, 2)))(
          isSubtype[AttributeValue.Map](
            hasField("value", _.value.keys.map(_.value).toSet, equalTo(Set("mouse_click")))
          )
        )
      },
      test("renamed case round-trips") {
        assertTrue(roundTrip[Event](Event.Click(1, 2)) == Right(Event.Click(1, 2)))
      },
      test("un-renamed case uses its original name as Map key") {
        val codec = codecFor[Event]
        assert(codec.encoder(Event.KeyPress(65)))(
          isSubtype[AttributeValue.Map](
            hasField("value", _.value.keys.map(_.value).toSet, equalTo(Set("KeyPress")))
          )
        )
      },
      test("un-renamed case round-trips") {
        assertTrue(roundTrip[Event](Event.KeyPress(65)) == Right(Event.KeyPress(65)))
      }
    )

  // ---- parameterised Scala 3 enum -----------------------------------------

  private val parameterisedEnumSuite =
    suite("parameterised Scala 3 enum")(
      test("Tree[Int] Leaf round-trips") {
        assertTrue(roundTrip[Tree[Int]](Tree.Leaf) == Right(Tree.Leaf))
      },
      test("Tree[Int] Branch with Leaf children round-trips") {
        val tree: Tree[Int] = Tree.Branch(42, Tree.Leaf, Tree.Leaf)
        assertTrue(roundTrip(tree) == Right(tree))
      },
      test("Tree[Int] nested Branch round-trips") {
        val tree: Tree[Int] =
          Tree.Branch(1, Tree.Branch(2, Tree.Leaf, Tree.Leaf), Tree.Branch(3, Tree.Leaf, Tree.Leaf))
        assertTrue(roundTrip(tree) == Right(tree))
      },
      test("Tree[String] Branch round-trips") {
        val tree: Tree[String] = Tree.Branch("root", Tree.Leaf, Tree.Leaf)
        assertTrue(roundTrip(tree) == Right(tree))
      }
    )

  // ---- multi-level sealed trait hierarchy ---------------------------------

  private val multiLevelHierarchySuite =
    suite("multi-level sealed trait hierarchy with constant leaf cases")(
      test("constant cases at different hierarchy levels encode as their name string") {
        val codec = codecFor[Animal]
        assertTrue(
          codec.encoder(Cat) == AttributeValue.String("Cat") &&
            codec.encoder(Lion) == AttributeValue.String("Lion") &&
            codec.encoder(Tiger) == AttributeValue.String("Tiger")
        )
      },
      test("Cat round-trips") {
        assertTrue(roundTrip[Animal](Cat) == Right(Cat))
      },
      test("Lion round-trips (nested under BigCat)") {
        assertTrue(roundTrip[Animal](Lion) == Right(Lion))
      },
      test("Tiger round-trips (nested under BigCat)") {
        assertTrue(roundTrip[Animal](Tiger) == Right(Tiger))
      },
      test("decode error for unknown case name") {
        val codec = codecFor[Animal]
        assertTrue(codec.decoder(AttributeValue.String("Dog")).isLeft)
      }
    )

  // ---- Scala 3 union type with Key discriminator --------------------------

  private val unionTypeKeyDiscriminatorSuite =
    suite("Scala 3 union type with Key discriminator")(
      test("Int encodes as singleton Map keyed by 'scala.Int'") {
        type Value = Int | Boolean | String
        implicit val schema: Schema[Value] = Schema.derived
        val codec                          = codecFor[Value]
        assertTrue(
          codec.encoder(1) == AttributeValue.Map(Map(AttributeValue.String("scala.Int") -> AttributeValue.Number(1)))
        )
      },
      test("Boolean encodes as singleton Map keyed by 'scala.Boolean'") {
        type Value = Int | Boolean | String
        implicit val schema: Schema[Value] = Schema.derived
        val codec                          = codecFor[Value]
        assertTrue(
          codec.encoder(true) ==
            AttributeValue.Map(Map(AttributeValue.String("scala.Boolean") -> AttributeValue.Bool(true)))
        )
      },
      test("String encodes as singleton Map keyed by 'java.lang.String'") {
        type Value = Int | Boolean | String
        implicit val schema: Schema[Value] = Schema.derived
        val codec                          = codecFor[Value]
        assertTrue(
          codec.encoder("hi") ==
            AttributeValue.Map(Map(AttributeValue.String("java.lang.String") -> AttributeValue.String("hi")))
        )
      },
      test("round-trips Int") {
        type Value = Int | Boolean | String
        implicit val schema: Schema[Value] = Schema.derived
        assertTrue(roundTrip[Value](1) == Right(1))
      },
      test("round-trips Boolean") {
        type Value = Int | Boolean | String
        implicit val schema: Schema[Value] = Schema.derived
        assertTrue(roundTrip[Value](true) == Right(true))
      },
      test("round-trips String") {
        type Value = Int | Boolean | String
        implicit val schema: Schema[Value] = Schema.derived
        assertTrue(roundTrip[Value]("hello") == Right("hello"))
      },
      test("round-trips each member of a wider union type") {
        type Value = Int | Boolean | String | List[Int] | Unit
        implicit val schema: Schema[Value] = Schema.derived
        val codec                          = codecFor[Value]
        assertTrue(
          codec.decoder(codec.encoder(42)) == Right(42) &&
            codec.decoder(codec.encoder(false)) == Right(false) &&
            codec.decoder(codec.encoder("world")) == Right("world") &&
            codec.decoder(codec.encoder(List(1, 2, 3))) == Right(List(1, 2, 3)) &&
            codec.decoder(codec.encoder(())) == Right(())
        )
      },
      test("Tuple2 member encodes as singleton Map keyed by 'scala.Tuple2'") {
        type Value = Int | Boolean | String | (Int, Boolean) | List[Int] | Unit
        implicit val schema: Schema[Value] = Schema.derived
        val codec                          = codecFor[Value]
        assert(codec.encoder((1, true)))(
          isSubtype[AttributeValue.Map](
            hasField("value", _.value.keys.map(_.value).toSet, equalTo(Set("scala.Tuple2")))
          )
        )
      },
      test("Tuple2 member round-trips") {
        type Value = Int | Boolean | String | (Int, Boolean) | List[Int] | Unit
        implicit val schema: Schema[Value] = Schema.derived
        assertTrue(roundTrip[Value]((1, true)) == Right((1, true)))
      },
      test("decode error for unknown case key") {
        type Value = Int | Boolean | String
        implicit val schema: Schema[Value] = Schema.derived
        val codec                          = codecFor[Value]
        val badAv                          = AttributeValue.Map(
          Map(AttributeValue.String("scala.Long") -> AttributeValue.Number(1))
        )
        assertTrue(codec.decoder(badAv).isLeft)
      }
    )

  // ---- Scala 3 union type with DiscriminatorKind.None ---------------------

  private val unionTypeNoneDiscriminatorSuite =
    suite("Scala 3 union type with DiscriminatorKind.None")(
      test("Int encodes as AttributeValue.Number directly") {
        type Value = Int | Boolean | String
        implicit val schema: Schema[Value] = Schema.derived
        val codec                          = schema.deriving(DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.None)).derive
        assertTrue(codec.encoder(42) == AttributeValue.Number(42))
      },
      test("Boolean encodes as AttributeValue.Bool directly") {
        type Value = Int | Boolean | String
        implicit val schema: Schema[Value] = Schema.derived
        val codec                          = schema.deriving(DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.None)).derive
        assertTrue(codec.encoder(true) == AttributeValue.Bool(true))
      },
      test("String encodes as AttributeValue.String directly") {
        type Value = Int | Boolean | String
        implicit val schema: Schema[Value] = Schema.derived
        val codec                          = schema.deriving(DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.None)).derive
        assertTrue(codec.encoder("VVV") == AttributeValue.String("VVV"))
      },
      test("round-trips Int") {
        type Value = Int | Boolean | String
        implicit val schema: Schema[Value] = Schema.derived
        val codec                          = schema.deriving(DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.None)).derive
        assertTrue(codec.decoder(codec.encoder(1)) == Right(1))
      },
      test("round-trips Boolean") {
        type Value = Int | Boolean | String
        implicit val schema: Schema[Value] = Schema.derived
        val codec                          = schema.deriving(DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.None)).derive
        assertTrue(codec.decoder(codec.encoder(true)) == Right(true))
      },
      test("round-trips String") {
        type Value = Int | Boolean | String
        implicit val schema: Schema[Value] = Schema.derived
        val codec                          = schema.deriving(DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.None)).derive
        assertTrue(codec.decoder(codec.encoder("VVV")) == Right("VVV"))
      },
      test("decode error when no sub-codec matches") {
        type Value = Int | Boolean | String
        implicit val schema: Schema[Value] = Schema.derived
        val codec                          = schema.deriving(DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.None)).derive
        assertTrue(codec.decoder(AttributeValue.BinarySet(Iterable.empty)).isLeft)
      }
    )

  // ---- Scala 3 generic tuples (HList-style *:) ----------------------------

  private val genericTupleSuite =
    suite("Scala 3 generic tuples (*:)")(
      test("2-element tuple encodes as AttributeValue.List") {
        type T2 = Int *: String *: EmptyTuple
        implicit val schema: Schema[T2] = Schema.derived
        val codec                       = codecFor[T2]
        assert(codec.encoder(1 *: "hi" *: EmptyTuple))(
          isSubtype[AttributeValue.List](
            hasField(
              "value",
              _.value.toList,
              equalTo(List(AttributeValue.Number(BigDecimal(1)), AttributeValue.String("hi")))
            )
          )
        )
      },
      test("2-element tuple round-trips") {
        type T2 = Int *: String *: EmptyTuple
        implicit val schema: Schema[T2] = Schema.derived
        val value: T2                   = 42 *: "hello" *: EmptyTuple
        assertTrue(roundTrip(value) == Right(value))
      },
      test("4-element heterogeneous tuple round-trips") {
        type T4 = Byte *: Short *: Int *: Long *: EmptyTuple
        implicit val schema: Schema[T4] = Schema.derived
        val value: T4                   = (1: Byte) *: (2: Short) *: 3 *: 4L *: EmptyTuple
        assertTrue(roundTrip(value) == Right(value))
      },
      test("tuple with Boolean field round-trips") {
        type T3 = String *: Int *: Boolean *: EmptyTuple
        implicit val schema: Schema[T3] = Schema.derived
        val value: T3                   = "key" *: 99 *: true *: EmptyTuple
        assertTrue(roundTrip(value) == Right(value))
      }
    )

  // ---- nested generic records with inline derives Schema ------------------

  private val nestedGenericRecordSuite =
    suite("nested generic records with inline derives Schema")(
      test("Parent[Child[MySealedTrait]] round-trips") {
        case class Parent(child: Child[MySealedTrait]) derives Schema

        case class Child[T <: MySealedTrait](test: T) derives Schema

        sealed trait MySealedTrait derives Schema

        object MySealedTrait {
          case class Foo(foo: Int)    extends MySealedTrait
          case class Bar(bar: String) extends MySealedTrait
        }

        assertTrue(
          roundTrip[Parent](Parent(Child(MySealedTrait.Foo(1)))) ==
            Right(Parent(Child(MySealedTrait.Foo(1)))) &&
            roundTrip[Parent](Parent(Child(MySealedTrait.Bar("WWW")))) ==
            Right(Parent(Child(MySealedTrait.Bar("WWW"))))
        )
      }
    )

  // ---- IArray sequences ---------------------------------------------------

  private val iArraySuite =
    suite("IArray sequences")(
      test("IArray[Int] encodes as AttributeValue.List") {
        implicit val s: Schema[IArray[Int]] = Schema.derived
        val codec                           = codecFor[IArray[Int]]
        assert(codec.encoder(IArray(1, 2, 3)))(
          isSubtype[AttributeValue.List](
            hasField(
              "value",
              _.value.toList,
              equalTo(
                List(
                  AttributeValue.Number(BigDecimal(1)),
                  AttributeValue.Number(BigDecimal(2)),
                  AttributeValue.Number(BigDecimal(3))
                )
              )
            )
          )
        )
      },
      test("IArray[Int] round-trips by element comparison") {
        implicit val s: Schema[IArray[Int]] = Schema.derived
        val codec                           = codecFor[IArray[Int]]
        assertTrue(codec.decoder(codec.encoder(IArray(1, 2, 3))).map(_.toList) == Right(List(1, 2, 3)))
      },
      test("IArray[Long] round-trips by element comparison") {
        implicit val s: Schema[IArray[Long]] = Schema.derived
        val codec                            = codecFor[IArray[Long]]
        assertTrue(codec.decoder(codec.encoder(IArray(1L, 2L, 3L))).map(_.toList) == Right(List(1L, 2L, 3L)))
      },
      test("IArray[String] round-trips by element comparison") {
        implicit val s: Schema[IArray[String]] = Schema.derived
        val codec                              = codecFor[IArray[String]]
        assertTrue(
          codec.decoder(codec.encoder(IArray("A", "B", "C"))).map(_.toList) == Right(List("A", "B", "C"))
        )
      },
      test("record with empty IArray[String] field round-trips") {
        assertTrue(roundTrip(IArrayRecord(IArray())) == Right(IArrayRecord(IArray())))
      },
      test("record with non-empty IArray[String] field round-trips") {
        assertTrue(
          roundTrip(IArrayRecord(IArray("VVV", "WWW"))) == Right(IArrayRecord(IArray("VVV", "WWW")))
        )
      }
    )

  // ---- recursive parameterised enum with @Modifier.rename on case and fields

  private val renamedRecursiveEnumSuite =
    suite("recursive parameterised enum with @Modifier.rename on case and fields")(
      test("Cons encodes under renamed case key 'cons'") {
        val codec = codecFor[Chain[Int]]
        assert(codec.encoder(Chain.Cons(1, Chain.Nil)))(
          isSubtype[AttributeValue.Map](
            hasField("value", _.value.keys.map(_.value).toSet, equalTo(Set("cons")))
          )
        )
      },
      test("Cons inner Map uses renamed field keys 'hd' and 'tl'") {
        val codec = codecFor[Chain[Int]]
        assert(codec.encoder(Chain.Cons(1, Chain.Nil)))(
          isSubtype[AttributeValue.Map](
            hasField(
              "value",
              _.value.get(AttributeValue.String("cons")),
              isSome(
                isSubtype[AttributeValue.Map](
                  hasField(
                    "value",
                    _.value,
                    hasKey(AttributeValue.String("hd")) &&
                      hasKey(AttributeValue.String("tl")) &&
                      !hasKey(AttributeValue.String("head")) &&
                      !hasKey(AttributeValue.String("tail"))
                  )
                )
              )
            )
          )
        )
      },
      test("Chain.Nil round-trips") {
        assertTrue(roundTrip[Chain[Int]](Chain.Nil) == Right(Chain.Nil))
      },
      test("single-element Chain[Int] round-trips") {
        assertTrue(roundTrip[Chain[Int]](Chain.Cons(1, Chain.Nil)) == Right(Chain.Cons(1, Chain.Nil)))
      },
      test("multi-element Chain[Int] round-trips") {
        val chain: Chain[Int] = Chain.Cons(1, Chain.Cons(2, Chain.Nil))
        assertTrue(roundTrip(chain) == Right(chain))
      },
      test("Chain[String] round-trips") {
        val chain: Chain[String] = Chain.Cons("a", Chain.Cons("b", Chain.Nil))
        assertTrue(roundTrip(chain) == Right(chain))
      }
    )

  // ---- recursive ADT with DiscriminatorKind.None --------------------------

  private val recursiveADTNoneDiscriminatorSuite =
    suite("recursive parameterised enum with DiscriminatorKind.None")(
      test("Cons encodes using only its record fields — no wrapper key") {
        val codec =
          chainDoubleSchema.deriving(DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.None)).derive
        assert(codec.encoder(Chain.Cons(1.0, Chain.Nil)))(
          isSubtype[AttributeValue.Map](
            hasField(
              "value",
              _.value.keys.map(_.value).toSet,
              equalTo(Set("hd", "tl")) && !contains("cons")
            )
          )
        )
      },
      test("Chain.Nil encodes as empty AttributeValue.Map") {
        val codec =
          chainDoubleSchema.deriving(DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.None)).derive
        assertTrue(codec.encoder(Chain.Nil) == AttributeValue.Map(Map.empty))
      },
      test("single-element Chain[Double] round-trips") {
        val codec =
          chainDoubleSchema.deriving(DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.None)).derive
        assertTrue(codec.decoder(codec.encoder(Chain.Cons(1.0, Chain.Nil))) == Right(Chain.Cons(1.0, Chain.Nil)))
      },
      test("multi-element Chain[Double] round-trips") {
        val codec                =
          chainDoubleSchema.deriving(DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.None)).derive
        val chain: Chain[Double] = Chain.Cons(1.0, Chain.Cons(2.0, Chain.Nil))
        assertTrue(codec.decoder(codec.encoder(chain)) == Right(chain))
      },
      test("Chain.Nil round-trips with None discriminator") {
        val codec =
          chainDoubleSchema.deriving(DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.None)).derive
        assertTrue(codec.decoder(codec.encoder(Chain.Nil)) == Right(Chain.Nil))
      }
    )

  // ---- nested sealed variants with DiscriminatorKind.None -----------------

  private val nestedVariantsNoneDiscriminatorSuite =
    suite("nested sealed variants with DiscriminatorKind.None")(
      test("Case1 with various InnerValue types and Case2 all round-trip") {
        type InnerValue = Int | Boolean | String | List[Int]
        sealed trait Base
        case class Case1(value: InnerValue)        extends Base
        case class Case2(value: Map[String, Long]) extends Base
        given innerSch: Schema[InnerValue] = Schema.derived
        given baseSch: Schema[Base]        = Schema.derived
        val none                           = DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.None)
        val codec                          = summon[Schema[Base]].deriving(none).derive
        assertTrue(
          codec.decoder(codec.encoder(Case1(42))) == Right(Case1(42)) &&
            codec.decoder(codec.encoder(Case1(true))) == Right(Case1(true)) &&
            codec.decoder(codec.encoder(Case1("hello"))) == Right(Case1("hello")) &&
            codec.decoder(codec.encoder(Case1(List(1, 2)))) == Right(Case1(List(1, 2))) &&
            codec.decoder(codec.encoder(Case2(Map("k" -> 2L)))) == Right(Case2(Map("k" -> 2L))) &&
            codec.decoder(codec.encoder(Case2(Map.empty))) == Right(Case2(Map.empty))
        )
      }
    )

  // ---- dynamicValueCodec encoder for Scala 3 enum Variant types ----------
  //
  // dynamicValueCodec sees DynamicValue.Variant for all sealed-trait/enum cases.
  // Schema.toDynamicValue always wraps the inner value as DynamicValue.Record
  // (empty for no-field cases), so the encoder always uses Key-discriminator form:
  //   {"CaseName": {fields}}.
  //
  // NOTE: For true all-no-field Scala 3 enums (like Direction), the DDB codec
  // stores values as AttributeValue.String (enumValuesAsStrings=true), so the
  // Key-discriminator form produced here does NOT match the stored value and
  // cannot be used in condition expressions for those types. Data-carrying enums
  // (like Shape) use Key discriminator both for storage and here, so they work.

  private val dynamicValueVariantSuite =
    suite("dynamicValueCodec encoder — Scala 3 enum Variant types")(
      test("no-field enum case encodes as Key-discriminator Map with empty inner") {
        // Direction.North stored as "North" (enumValuesAsStrings=true), but
        // dynamicValueCodec lacks schema context so produces {"North": {}}.
        val dv = summon[Schema[Direction]].toDynamicValue(Direction.North)
        assert(dvCodec.encoder(dv))(
          isSubtype[AttributeValue.Map](
            hasField(
              "value",
              _.value.get(AttributeValue.String("North")),
              isSome(equalTo(AttributeValue.Map.empty))
            )
          )
        )
      },
      test("data-carrying enum case encodes as Key-discriminator Map with inner fields") {
        // Shape.Circle(5) stored as {"Circle": {"radius": 5}} — matches here.
        val dv = summon[Schema[Shape]].toDynamicValue(Shape.Circle(5))
        assert(dvCodec.encoder(dv))(
          isSubtype[AttributeValue.Map](
            hasField(
              "value",
              _.value.get(AttributeValue.String("Circle")),
              isSome(
                isSubtype[AttributeValue.Map](hasField("value", _.value, hasKey(AttributeValue.String("radius"))))
              )
            )
          )
        )
      },
      test("multi-field data-carrying enum case encodes all fields in inner Map") {
        val dv = summon[Schema[Shape]].toDynamicValue(Shape.Rect(10, 20))
        assert(dvCodec.encoder(dv))(
          isSubtype[AttributeValue.Map](
            hasField(
              "value",
              _.value.get(AttributeValue.String("Rect")),
              isSome(
                isSubtype[AttributeValue.Map](
                  hasField(
                    "value",
                    _.value,
                    hasKey(AttributeValue.String("width")) && hasKey(AttributeValue.String("height"))
                  )
                )
              )
            )
          )
        )
      }
    )

  // ---- Scala 3 opaque types -----------------------------------------------

  private val opaqueTypeSuite =
    suite("Scala 3 opaque types")(
      test("InvoiceId (opaque String) encodes as AttributeValue.String, not Map") {
        val codec = codecFor[InvoiceId]
        assertTrue(codec.encoder(InvoiceId("INV-001")) == AttributeValue.String("INV-001"))
      },
      test("InvoiceId round-trips") {
        val codec = codecFor[InvoiceId]
        assertTrue(codec.decoder(codec.encoder(InvoiceId("INV-001"))) == Right(InvoiceId("INV-001")))
      },
      test("Quantity (opaque Int) encodes as AttributeValue.Number, not Map") {
        val codec = codecFor[Quantity]
        assertTrue(codec.encoder(Quantity(42)) == AttributeValue.Number(BigDecimal(42)))
      },
      test("Quantity round-trips") {
        val codec = codecFor[Quantity]
        assertTrue(codec.decoder(codec.encoder(Quantity(42))) == Right(Quantity(42)))
      },
      test("record with opaque type fields encodes each field as its underlying primitive type") {
        val codec = codecFor[LineItem]
        assert(codec.encoder(LineItem(InvoiceId("INV-001"), Quantity(3))))(
          isSubtype[AttributeValue.Map](
            hasField[AttributeValue.Map, Option[AttributeValue]](
              "value",
              _.value.get(AttributeValue.String("invoiceId")),
              isSome(equalTo(AttributeValue.String("INV-001")))
            ) &&
              hasField[AttributeValue.Map, Option[AttributeValue]](
                "value",
                _.value.get(AttributeValue.String("qty")),
                isSome(equalTo(AttributeValue.Number(BigDecimal(3))))
              )
          )
        )
      },
      test("record with opaque type fields round-trips") {
        val item = LineItem(InvoiceId("INV-001"), Quantity(3))
        assertTrue(roundTrip(item) == Right(item))
      },
      test("decode error when wrong AttributeValue type is given for InvoiceId") {
        val codec = codecFor[InvoiceId]
        assertTrue(codec.decoder(AttributeValue.Number(BigDecimal(1))).isLeft)
      }
    )
}
