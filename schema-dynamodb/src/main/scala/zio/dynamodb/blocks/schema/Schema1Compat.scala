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

/**
 * Controls how the codec handles the format difference between the legacy
 * `zio-dynamodb` schema1 codec and the new `DynamoDBCodecDeriver` codec.
 *
 * The intended migration path is:
 *   1. `ReadBothWriteOld` — deploy the new library while still writing the
 *      old format; safe to run alongside the legacy library.
 *   2. `ReadBothWriteNew` — start writing the new format; old records still
 *      decode correctly via the fallback path.
 *   3. `ReadNewWriteNew` — once all data has been migrated; no fallback
 *      overhead at decode time.
 */
sealed abstract class Schema1Compat

object Schema1Compat {

  /** Write and read only the new format. Default for new deployments. */
  case object ReadNewWriteNew extends Schema1Compat

  /**
   * Write old (schema1) format; decode both old and new.
   * Use for phase-1 migration: the new library can be deployed alongside the
   * legacy library without breaking existing readers.
   */
  case object ReadBothWriteOld extends Schema1Compat

  /**
   * Write new format; decode both old and new.
   * Use for phase-2 migration: old records written by the legacy library are
   * still decoded correctly, while all new writes use the current format.
   */
  case object ReadBothWriteNew extends Schema1Compat
}
