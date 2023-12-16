package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ put, scanAll }
import zio.test.assertTrue
import zio.schema.Schema
import zio.schema.DeriveSchema
import zio.test.TestAspect
import zio.Chunk
object TypeSafeStreamingUtilsSpec extends DynamoDBLocalSpec {

  // This example migrates legacy data (forename) from the PersonLegacy table to another Person table
  // using the batchReadFromStream and batchWriteFromStream batched streaming utility functions
  final case class Person(id: String, surname: String, forename: Option[String], age: Int)
  object Person       {
    implicit val schema: Schema.CaseClass4[String, String, Option[String], Int, Person] = DeriveSchema.gen[Person]
    val (id, surname, forename, age)                                                    = ProjectionExpression.accessors[Person]
  }
  final case class PersonLegacy(id: String, forename: String)
  object PersonLegacy {
    implicit val schema: Schema.CaseClass2[String, String, PersonLegacy] = DeriveSchema.gen[PersonLegacy]
    val (id, height)                                                     = ProjectionExpression.accessors[PersonLegacy]
  }

  override def spec =
    suite("stream utils")(
      test("migrates legacy data using batchReadFromStream and batchWriteFromStream") {
        withTwoSingleIdKeyTables { (personTable, personLegacyTable) =>
          for {
            _            <- put(personTable, Person("1", "Smith", None, 21)).execute
            _            <- put(personTable, Person("2", "Brown", None, 42)).execute
            _            <- put(personTable, Person("3", "NotInLegacy", None, 18)).execute
            _            <- put(personLegacyTable, PersonLegacy("1", "John")).execute
            _            <- put(personLegacyTable, PersonLegacy("2", "Peter")).execute
            personStream <- scanAll[Person](personTable).execute
            migrated      = batchReadFromStream(personLegacyTable, personStream) { person =>
                              PersonLegacy.id.partitionKey === person.id
                            }.absolve.map { // we effectively do a left outer join here on the 2 tables
                              case (person, Some(legacy)) => person.copy(forename = Some(legacy.forename))
                              case (person, _)            => person
                            }
            _            <- batchWriteFromStream(migrated)(person => put(personTable, person)).runDrain
            migrated     <- scanAll[Person](personTable).execute.flatMap(_.runCollect)
          } yield assertTrue(
            migrated.sortBy(_.id) ==
              Chunk(
                Person("1", "Smith", Some("John"), 21),
                Person("2", "Brown", Some("Peter"), 42),
                Person("3", "NotInLegacy", None, 18)
              )
          )
        }
      }
    ) @@ TestAspect.nondeterministic @@ TestAspect.ignore

}
