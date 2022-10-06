package zio.dynamodb.examples

import zio.console.{ putStrLn, Console }
import zio.dynamodb.DynamoDBQuery.put
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }
import zio.stream.{ UStream, ZStream }
import zio.{ App, ExitCode, URIO }

object BatchFromStreamExamples extends App {

  final case class Person(id: Int, name: String)
  object Person {
    implicit val schema: Schema[Person] = DeriveSchema.gen[Person]
  }

  private val personStream: UStream[Person] =
    ZStream.fromIterable(1 to 20).map(i => Person(i, s"name$i"))

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
      _ <- TestDynamoDBExecutor.addTable("person", "id")
      // write to DB using the stream as the source of the data to write
      // note put query uses type safe API to save a Person case class directly using Schema derived codecs
      // write queries will automatically be batched using BatchWriteItem when calling DynamoDB
      _ <- batchWriteFromStream(personStream) { person =>
             put("person", Person(person.id, person.name))
           }.runDrain

      // read from the DB using the stream as the source of the primary key
      // read queries will automatically be batched using BatchGetItem when calling DynamoDB
      _ <- batchReadItemFromStream[Console, Person]("person", personStream)(person => PrimaryKey("id" -> person.id))
             .mapMPar(4)(item => putStrLn(s"item=$item"))
             .runDrain

      // same again but use Schema derived codecs to convert an Item to a Person
      _ <- batchReadFromStream[Console, Person]("person", personStream)(person => PrimaryKey("id" -> person.id))
             .mapMPar(4)(person => putStrLn(s"person=$person"))
             .runDrain
    } yield ()).provideCustomLayer(DynamoDBExecutor.test).exitCode
}
