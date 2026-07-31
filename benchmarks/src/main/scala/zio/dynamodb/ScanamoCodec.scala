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

import org.scanamo._
import org.scanamo.generic.semiauto._
import zio.dynamodb.BenchmarkDomain._

object ScanamoCodec {
  implicit val paymentMethod: DynamoFormat[PaymentMethod] = deriveDynamoFormat
  implicit val trafficLight: DynamoFormat[TrafficLight]   = deriveDynamoFormat
  implicit val tuple: DynamoFormat[(Int, Long, String)]   = deriveDynamoFormat
  implicit val person: DynamoFormat[Person]               = deriveDynamoFormat
}
