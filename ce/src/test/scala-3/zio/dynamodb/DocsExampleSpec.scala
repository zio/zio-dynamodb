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

import cats.effect.{ IO, IOApp, Resource }
import munit.FunSuite
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

/**
 * Mirrors docs/index.md's "See it in action" (Cats Effect) snippet exactly, as a
 *  compile-time check of that precise shape — a class/method body is type-checked whether
 *  or not it's ever instantiated/called, so this fails the build if the doc's code stops
 *  compiling. Not run against a real client (no Docker/Testcontainers dependency); the
 *  underlying CEInterpreter + dsl facade + .execute mechanics are already exercised for
 *  real elsewhere (CEDynamoDBSpec, CEHighLevelSpec).
 */
object DocsExampleObject extends IOApp.Simple {

  enum Genre derives Schema {
    case Drama, Comedy
  }

  case class Movie(id: String, genre: Genre) derives Schema

  object Movie extends CompanionOptics[Movie] {
    val id: Lens[Movie, String]   = $(_.id)
    val genre: Lens[Movie, Genre] = $(_.genre)
  }

  val client: Resource[IO, DynamoDbAsyncClient] =
    Resource.make(IO(DynamoDbAsyncClient.builder().build()))(c => IO(c.close()))

  def run: IO[Unit] =
    client.use { c =>
      given Interpreter[IO] = CEInterpreter.fromAsyncClient(c)
      for {
        _     <- put("movies", Movie("m1", Genre.Drama)).execute
        movie <- get("movies")(Movie.id.partitionKey === "m1").execute
        page  <- scan[Movie]("movies", 20).filter(Movie.genre === Genre.Drama).execute
      } yield ()
    }
}

class DocsExampleSpec extends FunSuite {
  test("docs/index.md CE example compiles") {
    // Compilation of DocsExampleObject above is the actual check; this just gives the
    // compile-check a visible place in test output.
    assert(true)
  }
}
