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

//package zio.dynamodb.proofs
//
//import zio.dynamodb.ProjectionExpression
//
//import scala.annotation.implicitNotFound
//
//@implicitNotFound(
//  "DynamoDB does not support the 'size' operation on type ${X}. This operation is only supported for collections that extends Iterable and String"
//)
//sealed trait Sizable[-X]
//trait SizableLowPriorityImplicits0 extends SizableLowPriorityImplicits1 {
//  implicit def unknown: Sizable[ProjectionExpression.Unknown] =
//    new Sizable[ProjectionExpression.Unknown] {}
//}
//trait SizableLowPriorityImplicits1 {
//  implicit def iterable[A]: Sizable[Iterable[A]] = new Sizable[Iterable[A]] {}
//  implicit def string[A]: Sizable[String]        = new Sizable[String] {}
//}
//object Sizable                     extends SizableLowPriorityImplicits0
