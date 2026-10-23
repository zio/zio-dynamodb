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

import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue => AwsAttrValue,
  CancellationReason => AwsCancellationReason,
  ConditionalCheckFailedException,
  TransactionCanceledException
}
import zio.test._

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import scala.collection.JavaConverters._

// Typed errors carry an item decoded from an AWS response and the SDK exception as getCause; test
// reporters, loggers and distributed runtimes Java-serialize exceptions, so all of it must round-trip.
object ErrorSerializationSpec extends ZIOSpecDefault {

  private def roundTrip[A](a: A): Either[Throwable, A] =
    scala.util.Try {
      val bytes = new ByteArrayOutputStream()
      val out   = new ObjectOutputStream(bytes)
      out.writeObject(a)
      out.close()
      val in    = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray)) {
        // sbt runs tests in layered class loaders; resolve classes the way the test itself sees them.
        override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] =
          Class.forName(desc.getName, false, ErrorSerializationSpec.getClass.getClassLoader)
      }
      in.readObject().asInstanceOf[A]
    }.toEither

  private def s(v: String): AwsAttrValue = AwsAttrValue.builder().s(v).build()

  private val awsItem: java.util.Map[String, AwsAttrValue] = Map(
    "id"     -> s("a"),
    "n"      -> AwsAttrValue.builder().n("42").build(),
    "bin"    -> AwsAttrValue.builder().b(SdkBytes.fromByteArray(Array[Byte](1, 2, 3))).build(),
    "binSet" -> AwsAttrValue
      .builder()
      .bs(SdkBytes.fromByteArray(Array[Byte](4)), SdkBytes.fromByteArray(Array[Byte](5)))
      .build(),
    "list"   -> AwsAttrValue.builder().l(s("x"), AwsAttrValue.builder().n("1").build()).build(),
    "map"    -> AwsAttrValue.builder().m(Map("k" -> s("v")).asJava).build(),
    "ss"     -> AwsAttrValue.builder().ss("p", "q").build(),
    "ns"     -> AwsAttrValue.builder().ns("1", "2").build(),
    "bool"   -> AwsAttrValue.builder().bool(true).build(),
    "null"   -> AwsAttrValue.builder().nul(true).build()
  ).asJava

  private val details =
    AwsErrorDetails.builder().errorCode("ConditionalCheckFailedException").serviceName("DynamoDb").build()

  private val conditionFailed: DynamoDBError.ItemError.ConditionFailed =
    AwsCodecs.fromConditionalCheckFailedException(
      ConditionalCheckFailedException
        .builder()
        .message("The conditional request failed")
        .item(awsItem)
        .awsErrorDetails(details)
        .statusCode(400)
        .requestId("req-1")
        .build()
        .asInstanceOf[ConditionalCheckFailedException]
    )

  private val transactionCancelled: DynamoDBError.TransactionError.TransactionCancelled =
    AwsCodecs.fromTransactionCanceledException(
      TransactionCanceledException
        .builder()
        .message("Transaction cancelled")
        .cancellationReasons(
          AwsCancellationReason.builder().code("ConditionalCheckFailed").message("failed").item(awsItem).build(),
          AwsCancellationReason.builder().code("None").build()
        )
        .awsErrorDetails(details)
        .statusCode(400)
        .requestId("req-2")
        .build()
        .asInstanceOf[TransactionCanceledException]
    )

  def spec = suite("ErrorSerialization")(
    test("an item decoded from an AWS response round-trips") {
      val item = AwsCodecs.fromAwsItem(awsItem)
      assertTrue(roundTrip(item) == Right(item))
    },
    test("items built locally round-trip, whichever JMapView builder made them") {
      val single  = Item("id" -> "a")
      val multi   = Item("id" -> "a", "n" -> 1, "flag" -> true)
      val linked  = AttrMap(
        JMapView.linked
          .builder[String, AttributeValue]
          .addOne("id", AttributeValue.String("a"))
          .addOne("n", AttributeValue.Number(BigDecimal(1)))
          .result
      )
      val updated = AttrMap(multi.map.updated("extra", AttributeValue.String("x")))
      assertTrue(
        roundTrip(single) == Right(single),
        roundTrip(multi) == Right(multi),
        roundTrip(linked) == Right(linked),
        roundTrip(updated) == Right(updated)
      )
    },
    test("ConditionFailed round-trips, item and AWS cause included") {
      val result = roundTrip(conditionFailed)
      assertTrue(
        result.map(e => (e.message, e.item)) == Right((conditionFailed.message, conditionFailed.item)),
        result.toOption.exists(_.getCause.isInstanceOf[ConditionalCheckFailedException])
      )
    },
    test("TransactionCancelled round-trips, reasons' items and AWS cause included") {
      val result = roundTrip(transactionCancelled)
      assertTrue(
        result.map(_.reasons) == Right(transactionCancelled.reasons),
        result.toOption.exists(_.getCause.isInstanceOf[TransactionCanceledException])
      )
    }
  )
}
