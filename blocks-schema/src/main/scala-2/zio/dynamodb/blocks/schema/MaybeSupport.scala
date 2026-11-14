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

import zio.blocks.maybe.MaybeValue

private[schema] object MaybeSupport {
  def absent: AnyRef = MaybeValue.Absent

  def isAbsent(value: AnyRef): Boolean = value eq MaybeValue.Absent

  def innerValue(value: AnyRef): AnyRef =
    value.asInstanceOf[MaybeValue[AnyRef]] match {
      case MaybeValue.Present(v) => v.asInstanceOf[AnyRef]
      case MaybeValue.Absent     => null
    }

  def present(innerValue: AnyRef): AnyRef = MaybeValue.Present(innerValue)
}
