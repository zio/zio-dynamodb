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

import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.{ AttributeValue, Decoder, Encoder }
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }
import zio.dynamodb.blocks.schema.ddbexpr.DdbExpr
import zio.dynamodb.blocks.schema.ddbexpr.dsl._

/**
 * A hand-written codec for one field, swapped in with `.instance(optic, codec)`, that pattern
 * matches `AttributeValue`'s cases directly. Suppose your app originally stored `priority` as a
 * number (0 = Low, 1 = High) and later switched to the readable `"Low"` / `"High"`. This codec
 * reads both and always writes the string form, so items move to it as they are rewritten.
 * (This is a change in the app's own data format - the library's own 2.x -> 3.x encoding
 * changes are handled by the `Schema1Compat` knobs instead.)
 */
object HandWrittenCodecs {

  sealed trait Priority
  object Priority {
    case object Low  extends Priority
    case object High extends Priority
    implicit val schema: Schema[Priority] = Schema.derived
  }

  final case class Ticket(id: String, priority: Priority)
  object Ticket extends CompanionOptics[Ticket] {
    implicit val schema: Schema[Ticket]  = Schema.derived
    val id: Lens[Ticket, String]         = $(_.id)
    val priority: Lens[Ticket, Priority] = $(_.priority)
  }

  val priorityCodec: DynamoDBCodec[Priority] = new DynamoDBCodec[Priority] {
    def encoder: Encoder[Priority] = p => AttributeValue.String(p.toString)

    def decoder: Decoder[Priority] = {
      case AttributeValue.String("High") => Right(Priority.High)
      case AttributeValue.String("Low")  => Right(Priority.Low)
      case AttributeValue.Number(n)      => Right(if (n == 0) Priority.Low else Priority.High) // earlier format
      case other                         => Left(DecodingError(s"not a Priority: ${other.showType}"))
    }
  }

  val ticketCodec: DynamoDBCodec[Ticket] =
    Ticket.schema.deriving(DynamoDBCodecDeriver).instance(Ticket.priority, priorityCodec).derive

  val tickets: Table[Ticket] = Table("tickets", ticketCodec)

  // Expressions encode their literals with the same codec, so this compares against "High"
  val highPriority: DdbExpr[Ticket, Boolean] = Ticket.priority === Priority.High
}
