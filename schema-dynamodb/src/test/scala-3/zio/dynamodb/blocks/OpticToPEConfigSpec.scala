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

import zio.blocks.schema.{ CompanionOptics, Lens, Modifier, NameMapper, Schema }
import zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure
import zio.test._

/**
 * Slice 2a — config-aware `OpticToPE.pe(optic, reflect, config)`. Every `Field` segment
 * must resolve to the same DynamoDB attribute name the item-body codec writes; the parity
 * checks below compare against a real `recordFieldNameMap` from the deriver.
 */
object OpticToPEConfigSpec extends ZIOSpecDefault {

  // Address carries a per-type @Modifier.fieldNaming — must override a plain config mapper
  // for its own fields, but not for the segments above it.
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

  private def resolve(lens: Lens[Order, ?], cfg: DynamoDBCodecDeriverConfigure[Order]): Either[String, String] =
    OpticToPE.pe(lens, summon[Schema[Order]].reflect, cfg).map(_.toString)

  private def bodyName[A: Schema](scalaField: String, cfg: DynamoDBCodecDeriverConfigure[A]): String =
    summon[Schema[A]].deriving(cfg.toDeriver).derive.recordFieldNameMap(scalaField)

  def spec = suite("OpticToPE — config-aware")(
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
        // shipping -> shipping, address -> address, zipCode -> zip_code (annotation and mapper agree here)
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
    )
  )
}
