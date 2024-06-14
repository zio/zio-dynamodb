package zio.dynamodb.examples

import zio.dynamodb._
import zio.dynamodb.DynamoDBQuery.put
import zio.schema.{ DeriveSchema, Schema }
import zio.stream.{ UStream, ZStream }
import zio.ZIOAppDefault
import zio.Console.printLine
import zio.ZIO

object BatchFromStreamExamples extends ZIOAppDefault {

  final case class Person(id: Int, name: String)
  object Person {
    implicit val schema: Schema.CaseClass2[Int, String, Person] = DeriveSchema.gen[Person]
    val (id, name)                                              = ProjectionExpression.accessors[Person]
  }

  private val personIdStream: UStream[Int] =
    ZStream.fromIterable(1 to 20)

  private val personStream: UStream[Person] =
    ZStream.fromIterable(1 to 20).map(i => Person(i, s"name$i"))

  override def run: ZIO[Any, Throwable, Unit] =
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
      _ <- batchReadItemFromStream("person", personIdStream)(id => PrimaryKey("id" -> id))
             .mapZIOPar(4)(item => printLine(s"item=$item"))
             .runDrain

      // same again but use Schema derived codecs to convert an Item to a Person
      _ <- batchReadFromStream("person", personIdStream)(id => Person.id.partitionKey === id)
             .mapZIOPar(4)(person => printLine(s"person=$person"))
             .runDrain
    } yield ()).provide(DynamoDBExecutor.test)
}
