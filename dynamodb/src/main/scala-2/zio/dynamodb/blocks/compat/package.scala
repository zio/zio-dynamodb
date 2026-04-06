package zio.dynamodb.blocks

import zio.blocks.schema.comptime.Allows
import Allows._

package object compat {
  type Or[A <: Allows.Structural, B <: Allows.Structural] = A `|` B
}
