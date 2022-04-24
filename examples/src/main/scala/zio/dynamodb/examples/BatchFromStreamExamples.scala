package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery.putItem
import zio.dynamodb.{
  batchReadFromStream,
  batchReadItemFromStream,
  batchWriteFromStream,
  DynamoDBExecutor,
  Item,
  PrimaryKey,
  TestDynamoDBExecutor
}
import zio.schema.{ DeriveSchema, Schema }
import zio.stream.{ UStream, ZStream }
import zio.ZIOAppDefault
import zio.Console.printLine

object BatchFromStreamExamples extends ZIOAppDefault {

  final case class Person(id: Int, name: String)
  object Person {
    implicit val schema: Schema[Person] = DeriveSchema.gen[Person]
  }

  private val personStream: UStream[Person] =
    ZStream.fromIterable(1 to 20).map(i => Person(i, s"name$i"))

  override def run =
    (for {
      _ <- TestDynamoDBExecutor.addTable("person", "id")
      // write to DB using the stream as the source of the data to write
      // write queries will automatically be batched using BatchWriteItem when calling DynamoDB
      _ <- batchWriteFromStream(personStream) { person =>
             putItem("person", Item("id" -> person.id, "name" -> person.name))
           }.runDrain

      // read from the DB using the stream as the source of the primary key
      // read queries will automatically be batched using BatchGetItem when calling DynamoDB
      _ <- batchReadItemFromStream[Any, Person]("person", personStream)(person => PrimaryKey("id" -> person.id))
             .mapZIOPar(4)(item => printLine(s"item=$item"))
             .runDrain

      // same again but use Schema derived codecs to convert an Item to a Person
      _ <- batchReadFromStream[Any, Person]("person", personStream)(person => PrimaryKey("id" -> person.id))
             .mapZIOPar(4)(person => printLine(s"person=$person"))
             .runDrain
    } yield ()).provide(DynamoDBExecutor.test)
}
