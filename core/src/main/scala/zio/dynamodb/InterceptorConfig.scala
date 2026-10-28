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

/**
 * The optional interceptors a `fromAsyncClient` factory accepts. Each defaults to `None`; set only
 * the ones you need: `InterceptorConfig(retry = Some(myRetryInterceptor))`.
 */
final case class InterceptorConfig[F[_]](
  response: Option[ResponseInterceptor[F]] = None,
  retry: Option[RetryInterceptor[F]] = None,
  batchUnprocessedRetry: Option[BatchUnprocessedRetryInterceptor[F]] = None
)
