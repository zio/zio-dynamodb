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

package zio.dynamodb.blocks

import zio.blocks.maybe.Maybe
import zio.blocks.schema.*
import zio.blocks.schema.derive.Deriver
import zio.blocks.schema.json.DiscriminatorKind
import zio.dynamodb.{ AttributeValue, Decoder, Encoder, FromAttributeValue, ToAttributeValue }
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }
import zio.test.*

object CodecNavigatorSpec extends ZIOSpecDefault {

  // ── Models ───────────────────────────────────────────────────────────────────

  final case class Sku(value: String)
  object Sku { given Schema[Sku] = Schema[String].transform[Sku](Sku(_), _.value) }

  final case class GeoPoint(lat: Double, lng: Double) derives Schema
  final case class Address(street: String, geo: GeoPoint) derives Schema

  final case class Parcel(id: String, to: Maybe[Address]) derives Schema

  final case class Prefs(id: Int, counts: Map[String, Int]) derives Schema

  sealed trait Delivery derives Schema
  final case class Courier(address: Address) extends Delivery derives Schema
  final case class Pickup(lockerId: String)  extends Delivery derives Schema

  final case class Line(sku: Sku, qty: Int) derives Schema

  final case class Order(
    id: String,
    customerName: String,
    delivery: Delivery,
    alt: Option[Address],
    lines: List[Line],
    places: Map[String, Address],
    labels: Set[String],
    pair: (String, Int)
  ) derives Schema

  object Order extends CompanionOptics[Order] {
    val id: Lens[Order, String]                   = $(_.id)
    val customerName: Lens[Order, String]         = $(_.customerName)
    val delivery: Lens[Order, Delivery]           = $(_.delivery)
    val courier: Optional[Order, Courier]         = $(_.delivery.when[Courier])
    val courierStreet: Optional[Order, String]    = $(_.delivery.when[Courier].address.street)
    val courierLat: Optional[Order, Double]       = $(_.delivery.when[Courier].address.geo.lat)
    val pickupLocker: Optional[Order, String]     = $(_.delivery.when[Pickup].lockerId)
    val altStreet: Optional[Order, String]        = $(_.alt.when[Some[Address]].value.street)
    val lineQty: Optional[Order, Int]             = $(_.lines.at(1).qty)
    val lineSku: Optional[Order, Sku]             = $(_.lines.at(0).sku)
    val placeStreet: Optional[Order, String]      = $(_.places.atKey("home").street)
    val labels: Lens[Order, Set[String]]          = $(_.labels)
    val lines: Lens[Order, List[Line]]            = $(_.lines)
    val places: Lens[Order, Map[String, Address]] = $(_.places)
  }

  @Modifier.discriminator("kind")
  sealed trait Shape derives Schema
  @Modifier.rename("circ")
  final case class Circle(@Modifier.rename("r") radius: Double) extends Shape derives Schema
  final case class Square(side: Double)                         extends Shape derives Schema

  final case class Drawing(id: String, @Modifier.rename("main") shape: Shape) derives Schema
  object Drawing extends CompanionOptics[Drawing] {
    val circleRadius: Optional[Drawing, Double] = $(_.shape.when[Circle].radius)
    val squareSide: Optional[Drawing, Double]   = $(_.shape.when[Square].side)
  }

  sealed trait Payment derives Schema
  sealed trait Card                     extends Payment derives Schema
  final case class Visa(number: String) extends Card derives Schema
  final case class Cash(amount: Int)    extends Payment derives Schema
  final case class Bill(id: String, payment: Payment) derives Schema
  object Bill                           extends CompanionOptics[Bill] {
    val visaNumber: Optional[Bill, String] = $(_.payment.when[Card].when[Visa].number)
    val cashAmount: Optional[Bill, Int]    = $(_.payment.when[Cash].amount)
  }

  sealed trait Colour derives Schema
  case object Red  extends Colour
  case object Blue extends Colour
  final case class Paint(id: String, colour: Colour) derives Schema
  object Paint     extends CompanionOptics[Paint] {
    val red: Optional[Paint, Red.type] = $(_.colour.when[Red.type])
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def navigate(codec: DynamoDBCodec[_], optic: Optic[_, _]): Either[String, String] =
    CodecNavigator.resolve(codec, optic.toDynamic).map(_.path.toString)

  private val parityPaths: List[Optic[Order, ?]] = List(
    Order.id,
    Order.customerName,
    Order.delivery,
    Order.courier,
    Order.courierStreet,
    Order.courierLat,
    Order.pickupLocker,
    Order.altStreet,
    Order.lineQty,
    Order.lineSku,
    Order.placeStreet
  )

  private val orderTypeId   = Schema[Order].reflect.typeId
  private val addressTypeId = Schema[Address].reflect.typeId

  // Expected paths, in `parityPaths` order: what each configured codec writes.
  private val configs: List[(String, Deriver[DynamoDBCodec], List[String])] = List(
    (
      "default",
      DynamoDBCodecDeriver,
      List(
        "id",
        "customerName",
        "delivery",
        "delivery.Courier",
        "delivery.Courier.address.street",
        "delivery.Courier.address.geo.lat",
        "delivery.Pickup.lockerId",
        "alt.street",
        "lines[1].qty",
        "lines[0].sku",
        "places.home.street"
      )
    ),
    (
      "snake case fields",
      DynamoDBCodecDeriver.withFieldNameMapper(NameMapper.SnakeCase),
      List(
        "id",
        "customer_name",
        "delivery",
        "delivery.Courier",
        "delivery.Courier.address.street",
        "delivery.Courier.address.geo.lat",
        "delivery.Pickup.locker_id",
        "alt.street",
        "lines[1].qty",
        "lines[0].sku",
        "places.home.street"
      )
    ),
    (
      "kebab fields + cases",
      DynamoDBCodecDeriver
        .withFieldNameMapper(NameMapper.KebabCase)
        .withCaseNameMapper(NameMapper.KebabCase),
      List(
        "id",
        "customer-name",
        "delivery",
        "delivery.courier",
        "delivery.courier.address.street",
        "delivery.courier.address.geo.lat",
        "delivery.pickup.locker-id",
        "alt.street",
        "lines[1].qty",
        "lines[0].sku",
        "places.home.street"
      )
    ),
    (
      "type-keyed renames",
      DynamoDBCodecDeriver
        .withModifier(addressTypeId, "street", Modifier.rename("road"))
        .withModifier(orderTypeId, "delivery", Modifier.rename("dlv")),
      List(
        "id",
        "customerName",
        "dlv",
        "dlv.Courier",
        "dlv.Courier.address.road",
        "dlv.Courier.address.geo.lat",
        "dlv.Pickup.lockerId",
        "alt.road",
        "lines[1].qty",
        "lines[0].sku",
        "places.home.road"
      )
    ),
    (
      "Field discriminator",
      DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.Field("type")),
      List(
        "id",
        "customerName",
        "delivery",
        "delivery",
        "delivery.address.street",
        "delivery.address.geo.lat",
        "delivery.lockerId",
        "alt.street",
        "lines[1].qty",
        "lines[0].sku",
        "places.home.street"
      )
    )
  )

  private val upper: DynamoDBCodec[String] = new DynamoDBCodec[String] {
    def encoder: Encoder[String] = s => ToAttributeValue[String].toAttributeValue(s.toUpperCase)
    def decoder: Decoder[String] = av => FromAttributeValue[String].fromAttributeValue(av)
  }

  private val courier = Courier(Address("Main St", GeoPoint(51.5, -0.1)))
  private val order   = Order(
    "o-1",
    "Ann",
    courier,
    Some(Address("High St", GeoPoint(0, 0))),
    List(Line(Sku("s-1"), 2)),
    Map("home" -> Address("Low St", GeoPoint(1, 1))),
    Set("x"),
    ("p", 1)
  )

  // ── Spec ─────────────────────────────────────────────────────────────────────

  def spec = suite("CodecNavigator")(
    suite("configured paths")(
      configs.map { case (name, cfg, expected) =>
        test(name) {
          val codec = Schema[Order].deriving(cfg).derive
          assertTrue(parityPaths.map(navigate(codec, _)) == expected.map(Right(_)))
        }
      } :+
        test("field / case renames and a Field discriminator from annotations") {
          val cfg   = DynamoDBCodecDeriver
          val codec = Schema[Drawing].deriving(cfg).derive
          assertTrue(
            navigate(codec, Drawing.circleRadius) == Right("main.r"),
            navigate(codec, Drawing.squareSide) == Right("main.side")
          )
        }
    ),
    suite("paths that follow the actual wire shape")(
      test("a map key is encoded with the key codec, as the stored item's keys are") {
        val codec = Schema[Prefs].deriving(DynamoDBCodecDeriver).instance(Schema[String].reflect.typeId, upper).derive
        val item  = codec.toItem(Prefs(1, Map("home" -> 2)))
        val path  = CodecNavigator.resolve(
          codec,
          DynamicOptic(
            IndexedSeq(
              DynamicOptic.Node.Field("counts"),
              DynamicOptic.Node.AtMapKey(DynamicValue.Primitive(PrimitiveValue.String("home")))
            )
          )
        )
        assertTrue(path.map(_.path.toString) == Right("counts.HOME")) &&
        assert(item.map.get("counts"))(
          Assertion.isSome(
            Assertion.isSubtype[AttributeValue.Map](
              Assertion.hasField[AttributeValue.Map, Set[AttributeValue.String]](
                "keys",
                _.value.keySet.toSet,
                Assertion.equalTo(Set(AttributeValue.String("HOME")))
              )
            )
          )
        )
      },
      test("a Maybe is transparent on the wire, so its Present case and value add no segment") {
        val codec = Schema[Parcel].deriving(DynamoDBCodecDeriver).derive
        val item  = codec.toItem(Parcel("p-1", Maybe.present(Address("Main St", GeoPoint(0, 0)))))
        val path  = CodecNavigator.resolve(
          codec,
          DynamicOptic(
            IndexedSeq(
              DynamicOptic.Node.Field("to"),
              DynamicOptic.Node.Case("Present"),
              DynamicOptic.Node.Field("value"),
              DynamicOptic.Node.Field("street")
            )
          )
        )
        assertTrue(path.map(_.path.toString) == Right("to.street")) &&
        assert(item.map.get("to"))(
          Assertion.isSome(
            Assertion.isSubtype[AttributeValue.Map](
              Assertion.hasField[AttributeValue.Map, Option[AttributeValue]](
                "value",
                _.value.get(AttributeValue.String("street")),
                Assertion.isSome(Assertion.equalTo(AttributeValue.String("Main St")))
              )
            )
          )
        )
      },
      test("a tuple is a positional list, so a tuple field is a list index") {
        val codec = Schema[Order].deriving(DynamoDBCodecDeriver).derive
        val item  = codec.toItem(order)
        val path  = CodecNavigator.resolve(codec, DynamicOptic.root.field("pair").field("_2"))
        assertTrue(
          path.map(_.path.toString) == Right("pair[1]"),
          item.map.get("pair").contains(AttributeValue.List(List(AttributeValue.String("p"), AttributeValue.Number(1))))
        )
      },
      test("a nested sealed trait is flattened on the wire, so naming it adds no segment") {
        val codec = Schema[Bill].deriving(DynamoDBCodecDeriver).derive
        val item  = codec.toItem(Bill("b-1", Visa("4111")))
        assertTrue(
          navigate(codec, Bill.visaNumber) == Right("payment.Visa.number"),
          navigate(codec, Bill.cashAmount) == Right("payment.Cash.amount"),
          item.map.get("payment").exists(_.toString.contains("Visa"))
        )
      },
      test("an enumeration is a plain string, so a case has no path") {
        val codec = Schema[Paint].deriving(DynamoDBCodecDeriver).derive
        assertTrue(navigate(codec, Paint.red).isLeft)
      },
      test("a DynamoDB set has no index") {
        val codec = Schema[Order].deriving(DynamoDBCodecDeriver).derive
        assertTrue(CodecNavigator.resolve(codec, DynamicOptic.root.field("labels").at(0)).isLeft)
      }
    ),
    suite("the codec at a position")(
      test("ZB optic overrides: the path is renamed and the codec there is the override") {
        val codec  = Schema[Order]
          .deriving(DynamoDBCodecDeriver)
          .instance(Order.courierStreet, upper)
          .modifier(Order.courierLat, Modifier.rename("latitude"))
          .derive
        val street = CodecNavigator.resolve(codec, Order.courierStreet.toDynamic)
        assertTrue(
          street.map(_.path.toString) == Right("delivery.Courier.address.street"),
          street.map(_.codec.encoder("Main St")) == Right(AttributeValue.String("MAIN ST")),
          navigate(codec, Order.courierLat) == Right("delivery.Courier.address.geo.latitude")
        )
      },
      test("an optic rename applies to its position only") {
        val codec = Schema[Order]
          .deriving(DynamoDBCodecDeriver)
          .modifier(Order.courierStreet, Modifier.rename("road"))
          .derive
        assertTrue(
          navigate(codec, Order.courierStreet) == Right("delivery.Courier.address.road"),
          navigate(codec, Order.altStreet) == Right("alt.street")
        )
      },
      test("a literal at an ancestor encodes exactly as the item does") {
        val codec    = Schema[Order]
          .deriving(DynamoDBCodecDeriver)
          .instance(Order.courierStreet, upper)
          .modifier(Order.courierLat, Modifier.rename("latitude"))
          .derive
        val delivery = CodecNavigator.resolve(codec, Order.delivery.toDynamic)
        assertTrue(delivery.map(_.codec.encoder(courier)).toOption == codec.toItem(order).map.get("delivery"))
      },
      test("list element, map value, set element and wrapper positions") {
        val codec = Schema[Order].deriving(DynamoDBCodecDeriver).derive
        val line  = CodecNavigator.resolve(codec, Order.lines.toDynamic.apply(DynamicOptic.elements))
        val place = CodecNavigator.resolve(codec, Order.places.toDynamic.apply(DynamicOptic.mapValues))
        val label = CodecNavigator.resolve(codec, Order.labels.toDynamic.apply(DynamicOptic.elements))
        val sku   = CodecNavigator.resolve(codec, Order.lineSku.toDynamic)
        assertTrue(
          line.map(r => (r.path.toString, r.codec.encoder(Line(Sku("s-9"), 3)))).toOption ==
            codec.toItem(order.copy(lines = List(Line(Sku("s-9"), 3)))).map.get("lines").collect {
              case AttributeValue.List(items) => ("lines", items.head)
            },
          place.map(_.path.toString) == Right("places"),
          label.map(_.codec.encoder("x")) == Right(AttributeValue.String("x")),
          sku.map(r => (r.path.toString, r.codec.encoder(Sku("s-1")))) == Right(
            ("lines[0].sku", AttributeValue.String("s-1"))
          )
        )
      }
    ),
    suite("errors")(
      test("missing fields and cases, and nodes of the wrong shape") {
        val codec = Schema[Order].deriving(DynamoDBCodecDeriver).derive
        assertTrue(
          CodecNavigator
            .resolve(codec, DynamicOptic.root.field("nope"))
            .left
            .toOption
            .contains("field 'nope' not found"),
          CodecNavigator
            .resolve(codec, DynamicOptic.root.field("delivery").caseOf("Drone"))
            .left
            .toOption
            .contains("case 'Drone' not found"),
          CodecNavigator
            .resolve(codec, DynamicOptic.root.field("id").field("x"))
            .left
            .toOption
            .contains("path segment 'x' is not a record field")
        )
      },
      test("DiscriminatorKind.None has no path through a case") {
        val codec = Schema[Order].deriving(DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.None)).derive
        assertTrue(
          navigate(codec, Order.courierStreet).left.toOption
            .contains("cannot build a path through a variant case with DiscriminatorKind.None")
        )
      },
      test("a hand-written codec can't be navigated") {
        assertTrue(
          CodecNavigator
            .resolve(upper, DynamicOptic.root.field("x"))
            .left
            .toOption
            .contains("path segment 'x' is not a record field")
        )
      }
    )
  )
}
