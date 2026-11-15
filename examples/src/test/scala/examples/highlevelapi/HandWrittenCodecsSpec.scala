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

package examples.highlevelapi

import examples.highlevelapi.HandWrittenCodecs._
import zio.dynamodb.AttributeValue
import zio.test._

object HandWrittenCodecsSpec extends ZIOSpecDefault {

  private def item(id: String, priority: AttributeValue): AttributeValue =
    AttributeValue.Map(
      Map(AttributeValue.String("id") -> AttributeValue.String(id), AttributeValue.String("priority") -> priority)
    )

  def spec = suite("HandWrittenCodecs")(
    test("writes priority as a String") {
      assertTrue(
        ticketCodec.encoder(Ticket("t-1", Priority.High)) == item("t-1", AttributeValue.String("High"))
      )
    },
    test("reads the current String form") {
      assertTrue(
        ticketCodec.decoder(item("t-1", AttributeValue.String("Low"))) == Right(Ticket("t-1", Priority.Low))
      )
    },
    test("reads the earlier Number form") {
      assertTrue(
        ticketCodec.decoder(item("t-1", AttributeValue.Number(BigDecimal(0)))) == Right(Ticket("t-1", Priority.Low)),
        ticketCodec.decoder(item("t-2", AttributeValue.Number(BigDecimal(1)))) == Right(Ticket("t-2", Priority.High))
      )
    },
    test("names the wire type it found when it can't decode") {
      val result = priorityCodec.decoder(AttributeValue.Bool(true))
      assertTrue(result.left.exists(_.message.contains("AttributeValue.Bool")))
    }
  )
}
