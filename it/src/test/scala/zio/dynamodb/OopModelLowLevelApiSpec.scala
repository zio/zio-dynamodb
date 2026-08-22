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

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio._
import zio.blocks.schema.{ Modifier, Schema }
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }
import zio.dynamodb.ProjectionExpression.$
import zio.test._
import zio.test.TestAspect

// Integration-test companion to
// schema-ddbexpr/src/test/scala/zio/dynamodb/OopModelWithAbstractFieldsSpec.scala.
// See docs/design/SumTypesWithAbstractFields.md for the full design discussion — in short:
// a root-level OO model with abstract/shared fields (`.id` on every case, `.amount` shared
// by the Billed cases) is only usable as a real top-level DynamoDB item at all — for *any*
// API, HL or LL — under `@Modifier.discriminator` (Field-style). Under this project's
// default Key-style, DynamoDBCodecDeriver wraps the *entire* encoded record under the leaf
// case's own name, so even a root-shared field like `id` ends up nested one level down —
// confirmed directly against a real table: `putItem` fails with "One of the required keys
// was not given a value", because `id` genuinely isn't a top-level attribute in the item.
// Field-style keeps every field flat and case-name-independent, so `id` is a valid
// partition key and `amount` matches uniformly across cases with a single filter — no
// per-case wrapper path, no manual Or, proven here against a real DynamoDB Local instance.
// Queried purely through the LL API + DynamoDBCodec.toItem/fromItem — this project's HL
// API optic sugar is deliberately never offered for abstract-declared fields (see the
// doc's TL;DR), so there's no ZB-side dependency here at all, local or published — and
// no Scala-3-only syntax either, hence plain (cross-compiled) source, not scala-3/.
@Modifier.discriminator("_type")
sealed trait OopInvoice { def id: Int }
object OopInvoice       {
  implicit val schema: Schema[OopInvoice] = Schema.derived
}

sealed trait OopBilled extends OopInvoice { def amount: Double }

case class OopBilledMonthly(id: Int, amount: Double, month: Int) extends OopBilled
object OopBilledMonthly {
  implicit val schema: Schema[OopBilledMonthly] = Schema.derived
}

case class OopBilledYearly(id: Int, amount: Double, year: Int) extends OopBilled
object OopBilledYearly {
  implicit val schema: Schema[OopBilledYearly] = Schema.derived
}

case class OopPrebilled(id: Int, count: Int) extends OopInvoice
object OopPrebilled {
  implicit val schema: Schema[OopPrebilled] = Schema.derived
}

object OopModelLowLevelApiSpec extends DynamoDBLocalSpec {

  private val codec: DynamoDBCodec[OopInvoice] = Schema[OopInvoice].deriving(DynamoDBCodecDeriver).derive

  // id is the model's Int partition key — a Number attribute, not the String key that
  // DynamoDBLocalSpec's own withSingleIdKeyTable assumes, so this test defines its own
  // table shape rather than reusing that helper.
  private def oopInvoiceTable(tableName: String): DynamoDBQuery[Any, Unit] =
    DynamoDBQuery.createTable(
      tableName,
      KeySchema("id"),
      NonEmptySet(AttributeDefinition.attrDefnNumber("id")),
      BillingMode.PayPerRequest
    )

  private def withOopInvoiceTable(
    f: (String, Interpreter[Task]) => ZIO[Any, Throwable, TestResult]
  ): ZIO[DynamoDBEnv, Throwable, TestResult] =
    ZIO.scoped {
      for {
        table  <- managedTable(oopInvoiceTable)
        env    <- ZIO.service[DynamoDBEnv]
        result <- f(table, env.interpreter)
      } yield result
    }

  private val envLayer: URLayer[DynamoDbAsyncClient, DynamoDBEnv] =
    ZLayer(ZIO.serviceWith[DynamoDbAsyncClient](client => DynamoDBEnv(client, ZioInterpreter.fromAsyncClient(client))))

  def spec =
    suite("OO model (Field-style discriminator) via the LL API against real DynamoDB Local")(
      test("id is a genuine top-level attribute — A -> Item -> A round-trips through putItem/getItem") {
        withOopInvoiceTable { (table, interpreter) =>
          val original: OopInvoice = OopBilledMonthly(1, 42.0, 3)
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, codec.toItem(original)))
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> 1)))
          } yield assertTrue(result.map(codec.fromItem) == Some(Right(original)))
        }
      },
      test("every case's id round-trips as a valid partition key, including the non-Billed case") {
        withOopInvoiceTable { (table, interpreter) =>
          val prebilled: OopInvoice = OopPrebilled(3, 5)
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, codec.toItem(prebilled)))
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> 3)))
          } yield assertTrue(result.map(codec.fromItem) == Some(Right(prebilled)))
        }
      },
      test("one flat filter on amount matches across every Billed case — no per-case wrapper, no manual Or") {
        withOopInvoiceTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, codec.toItem(OopBilledMonthly(1, 42.0, 3))))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, codec.toItem(OopBilledYearly(2, 42.0, 2024))))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, codec.toItem(OopPrebilled(3, 5))))
            page <- interpreter.run(DynamoDBQuery.scan(table, limit = 10).filter($("amount") === 42.0))
            decoded = page.items.map(codec.fromItem).toSet
          } yield assertTrue(
            page.items.length == 2,
            decoded == Set[Either[DynamoDBError.ItemError.DecodingError, OopInvoice]](
              Right(OopBilledMonthly(1, 42.0, 3)),
              Right(OopBilledYearly(2, 42.0, 2024))
            )
          )
        }
      },
      test("the same flat filter correctly excludes the case that has no amount field at all") {
        withOopInvoiceTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, codec.toItem(OopBilledMonthly(1, 42.0, 3))))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, codec.toItem(OopPrebilled(3, 5))))
            page <- interpreter.run(DynamoDBQuery.scan(table, limit = 10).filter($("amount") === 42.0))
            decoded = page.items.map(codec.fromItem)
          } yield assertTrue(
            page.items.length == 1,
            decoded == zio.blocks.chunk.Chunk(Right(OopBilledMonthly(1, 42.0, 3)))
          )
        }
      }
    ).provideSome[DynamoDbAsyncClient](envLayer) @@ TestAspect.sequential
}
