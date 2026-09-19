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

package examples

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.{ Task, ZIO, ZIOAppDefault, ZLayer }
import zio.blocks.schema.{ CompanionOptics, Lens, Optional, Schema }
import zio.dynamodb.{ Interpreter, ZioInterpreter }
import zio.dynamodb.ExecuteSyntax._
import zio.dynamodb.blocks.ddbexpr.dsl._

/**
 * ZIO version of the CRUD walkthrough — see `CrudWalkthroughCE.scala` for the Cats Effect
 * version of the same queries.
 */
object CrudWalkthroughZio extends ZIOAppDefault {

  case class Ticket(id: String, tags: Set[String], watchers: Option[List[String]])

  object Ticket extends CompanionOptics[Ticket] {
    implicit val schema: Schema[Ticket] = Schema.derived

    val id: Lens[Ticket, String]        = $(_.id)
    val tags: Lens[Ticket, Set[String]] = $(_.tags)

    def watcherAt(i: Int): Optional[Ticket, String] = $(_.watchers.when[Some[List[String]]].value.at(i))
  }

  val tickets: Table[Ticket] = Table[Ticket]("tickets")

  val interpreterLayer: ZLayer[Any, Throwable, Interpreter[Task]] =
    ZLayer.scoped {
      ZIO
        .acquireRelease(ZIO.attempt(DynamoDbAsyncClient.builder().build()))(c => ZIO.attempt(c.close()).orDie)
        .map(client => ZioInterpreter.fromAsyncClient(client): Interpreter[Task])
    }

  val program: ZIO[Interpreter[Task], Throwable, Unit] =
    ZIO.serviceWithZIO[Interpreter[Task]] { interpreter =>
      implicit val theInterpreter: Interpreter[Task] = interpreter
      for {
        _ <- put(tickets, Ticket("t1", Set("urgent"), Some(List("alice", "bob")))).execute

        // Add a tag and remove a watcher, in one update call.
        _ <- update(tickets)(Ticket.id.partitionKey === "t1")(
               Ticket.tags.addSet(Set("reviewed")) + Ticket.watcherAt(0).remove
             ).execute

        _ <- scan(tickets, limit = 20).filter(Ticket.watcherAt(0).attributeExists).execute
      } yield ()
    }

  def run: Task[Unit] = program.provide(interpreterLayer)
}
