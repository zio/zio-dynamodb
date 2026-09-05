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

import zio.blocks.schema.{
  CompanionOptics,
  DynamicOptic,
  DynamicValue,
  Lens,
  Modifier,
  NameMapper,
  PrimitiveValue,
  Schema
}
import zio.blocks.schema.json.DiscriminatorKind
import zio.dynamodb.{ AttributeValue, Decoder, DynamoDBError, Encoder }
import zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, Resolver }
import zio.test._

/**
 * Resolver refactor (`series/3.x_projection_resolver`) — same models and expectations as
 * `OpticToPEConfigSpec` (Slice 2a, superseded), now resolving through a deriver-produced
 * `Resolver` + `ProjectionResolver` instead of `OpticToPE`'s hand-walked schema + config.
 * Kept as a direct parity check while both mechanisms coexist during the migration.
 */
object ProjectionResolverSpec extends ZIOSpecDefault {

  @Modifier.fieldNaming("snake_case")
  final case class Address(streetName: String, zipCode: String) derives Schema

  final case class Shipping(carrier: String, address: Address) derives Schema

  final case class Order(customerId: String, orderRef: String, shipping: Shipping) derives Schema
  object Order extends CompanionOptics[Order] {
    val customerId: Lens[Order, String] = $(_.customerId)
    val orderRef: Lens[Order, String]   = $(_.orderRef)
    val zipPath: Lens[Order, String]    = $(_.shipping.address.zipCode)
  }

  private val orderTypeId = summon[Schema[Order]].reflect.typeId

  private def resolve(lens: Lens[Order, ?], cfg: DynamoDBCodecDeriverConfigure[Order]): Either[String, String] = {
    val root = summon[Schema[Order]].deriving(cfg.toResolverDeriver).derive
    new ProjectionResolver(root).resolve(lens.toDynamic).map(_.toString)
  }

  private def bodyName[A: Schema](scalaField: String, cfg: DynamoDBCodecDeriverConfigure[A]): String =
    summon[Schema[A]].deriving(cfg.toDeriver).derive.recordFieldNameMap(scalaField)

  def spec = suite("ProjectionResolver")(
    suite("top-level field, parity with the body codec")(
      test("withFieldNameMapper(SnakeCase)") {
        val cfg = DynamoDBCodecDeriverConfigure[Order]().withFieldNameMapper(NameMapper.SnakeCase)
        assertTrue(
          resolve(Order.customerId, cfg) == Right("customer_id"),
          resolve(Order.customerId, cfg) == Right(bodyName("customerId", cfg))
        )
      },
      test("withModifier rename") {
        val cfg = DynamoDBCodecDeriverConfigure[Order]()
          .withModifier(orderTypeId, "customerId", Modifier.rename("cust"))
        assertTrue(
          resolve(Order.customerId, cfg) == Right("cust"),
          resolve(Order.customerId, cfg) == Right(bodyName("customerId", cfg))
        )
      },
      test("default config is identity") {
        val cfg = DynamoDBCodecDeriverConfigure[Order]()
        assertTrue(
          resolve(Order.orderRef, cfg) == Right("orderRef"),
          resolve(Order.orderRef, cfg) == Right(bodyName("orderRef", cfg))
        )
      }
    ),
    suite("nested path")(
      test("config mapper applies per segment; Address's @Modifier.fieldNaming wins for its own fields") {
        val cfg = DynamoDBCodecDeriverConfigure[Order]().withFieldNameMapper(NameMapper.SnakeCase)
        assertTrue(resolve(Order.zipPath, cfg) == Right("shipping.address.zip_code"))
      },
      test("with default config the Address annotation still snake_cases its own leaf, upper segments stay raw") {
        val cfg = DynamoDBCodecDeriverConfigure[Order]()
        assertTrue(resolve(Order.zipPath, cfg) == Right("shipping.address.zip_code"))
      },
      test("nested leaf matches Address's own body codec") {
        val cfg        = DynamoDBCodecDeriverConfigure[Order]().withFieldNameMapper(NameMapper.SnakeCase)
        val nestedLeaf = resolve(Order.zipPath, cfg).map(_.split('.').last)
        val addressCfg = DynamoDBCodecDeriverConfigure[Address]().withFieldNameMapper(NameMapper.SnakeCase)
        assertTrue(nestedLeaf == Right(bodyName[Address]("zipCode", addressCfg)))
      }
    ),
    suite("resolver-specific")(
      test("root type itself derives to a Resolver.Record") {
        val root = summon[Schema[Order]].deriving(DynamoDBCodecDeriverConfigure[Order]().toResolverDeriver).derive
        assertTrue(root.isInstanceOf[Resolver.Record[Order]])
      }
    ),
    variantParitySuite,
    sequenceMapSuite,
    instanceOverrideSuite
  )

  // -- Variant / discriminator parity -----------------------------------------------------
  // Tracked carries a field (not a pure enum), so DiscriminatorKind actually affects encoding.

  sealed trait Shipment derives Schema
  object Shipment {
    final case class Tracked(carrier: String) extends Shipment derives Schema
    case object Untracked                     extends Shipment
  }
  final case class Parcel(id: String, shipment: Shipment) derives Schema

  private def navigate(av: AttributeValue, path: String): Option[AttributeValue] =
    path.split('.').foldLeft(Option(av)) { (cur, segment) =>
      cur.flatMap {
        case AttributeValue.Map(m) => m.get(AttributeValue.String(segment))
        case _                     => None
      }
    }

  private def variantPath(cfg: DynamoDBCodecDeriverConfigure[Parcel]): Either[String, String] = {
    val root = summon[Schema[Parcel]].deriving(cfg.toResolverDeriver).derive
    val dyn  = DynamicOptic(
      IndexedSeq(
        DynamicOptic.Node.Field("shipment"),
        DynamicOptic.Node.Case("Tracked"),
        DynamicOptic.Node.Field("carrier")
      )
    )
    new ProjectionResolver(root).resolve(dyn).map(_.toString)
  }

  private def variantParitySuite = suite("variant Case segment, parity with the actual encoding")(
    test("DiscriminatorKind.Key: case contributes a path segment") {
      val cfg     = DynamoDBCodecDeriverConfigure[Parcel]()
      val codec   = summon[Schema[Parcel]].deriving(cfg.toDeriver).derive
      val encoded = codec.encoder(Parcel("p1", Shipment.Tracked("dhl")))
      assertTrue(
        variantPath(cfg) == Right("shipment.Tracked.carrier"),
        navigate(encoded, "shipment.Tracked.carrier").contains(AttributeValue.String("dhl"))
      )
    },
    test("DiscriminatorKind.Field: case contributes no path segment - the discriminator is a sibling field") {
      val cfg     = DynamoDBCodecDeriverConfigure[Parcel]().withDiscriminatorKind(DiscriminatorKind.Field("type"))
      val codec   = summon[Schema[Parcel]].deriving(cfg.toDeriver).derive
      val encoded = codec.encoder(Parcel("p1", Shipment.Tracked("dhl")))
      assertTrue(
        variantPath(cfg) == Right("shipment.carrier"),
        navigate(encoded, "shipment.carrier").contains(AttributeValue.String("dhl")),
        navigate(encoded, "shipment.type").contains(AttributeValue.String("Tracked"))
      )
    }
  )

  // -- Sequence / Map pass-through ---------------------------------------------------------

  final case class Item(sku: String, qty: Int) derives Schema
  @Modifier.fieldNaming("snake_case")
  final case class Tagged(longName: String) derives Schema
  final case class Cart(items: List[Item], tags: scala.collection.immutable.Map[String, Tagged]) derives Schema

  private def sequenceMapSuite = suite("Sequence element and Map value resolve through to the nested type")(
    test("List[Item] element field") {
      val root = summon[Schema[Cart]].deriving(DynamoDBCodecDeriverConfigure[Cart]().toResolverDeriver).derive
      val dyn  = DynamicOptic(
        IndexedSeq(DynamicOptic.Node.Field("items"), DynamicOptic.Node.AtIndex(0), DynamicOptic.Node.Field("sku"))
      )
      assertTrue(new ProjectionResolver(root).resolve(dyn).map(_.toString) == Right("items[0].sku"))
    },
    test("Map[String, Tagged] value field - the nested type's own @Modifier.fieldNaming still applies") {
      val root = summon[Schema[Cart]].deriving(DynamoDBCodecDeriverConfigure[Cart]().toResolverDeriver).derive
      val dyn  = DynamicOptic(
        IndexedSeq(
          DynamicOptic.Node.Field("tags"),
          DynamicOptic.Node.AtMapKey(DynamicValue.Primitive(PrimitiveValue.String("k"))),
          DynamicOptic.Node.Field("longName")
        )
      )
      assertTrue(new ProjectionResolver(root).resolve(dyn).map(_.toString) == Right("tags.k.long_name"))
    }
  )

  // -- withInstance opacity ----------------------------------------------------------------

  final case class Weird(x: Int) derives Schema
  final case class Holder(weird: Weird, plain: String) derives Schema

  private def instanceOverrideSuite = suite("a codec instance override resolves as opaque")(
    test("a path through a withInstance-overridden type is Left, not a guessed name") {
      val weirdTypeId                      = summon[Schema[Weird]].reflect.typeId
      val dummyCodec: DynamoDBCodec[Weird] = new DynamoDBCodec[Weird] {
        def encoder: Encoder[Weird] = _ => AttributeValue.Null
        def decoder: Decoder[Weird] = _ => Left(DynamoDBError.ItemError.DecodingError.failure("dummy"))
      }
      val cfg                              = DynamoDBCodecDeriverConfigure[Holder]().withInstance(dummyCodec)(weirdTypeId)
      val root                             = summon[Schema[Holder]].deriving(cfg.toResolverDeriver).derive
      val dyn                              =
        DynamicOptic(IndexedSeq(DynamicOptic.Node.Field("weird"), DynamicOptic.Node.Field("x")))
      assertTrue(new ProjectionResolver(root).resolve(dyn).isLeft)
    }
  )
}
