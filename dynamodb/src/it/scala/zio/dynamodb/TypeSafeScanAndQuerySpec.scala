package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ put, queryAll, querySome, scanAll, scanSome }
import zio.Scope
import zio.test.Spec
import zio.test.assertTrue
import zio.test.TestEnvironment
import zio.schema.Schema
import zio.schema.DeriveSchema
import zio.Chunk
import zio.test.TestAspect

object TypeSafeScanAndQuerySpec extends DynamoDBLocalSpec {

  final case class Person(id: String, surname: String, forename: Option[String], age: Int)
  object Person {
    implicit val schema: Schema.CaseClass4[String, String, Option[String], Int, Person] = DeriveSchema.gen[Person]
    val (id, surname, forename, age)                                                    = ProjectionExpression.accessors[Person]
  }

  override def spec: Spec[Environment with TestEnvironment with Scope, Any] =
    suite("all")(scanAllSpec, scanSomeSpec, queryAllSpec, querySomeSpec) @@ TestAspect.nondeterministic

  private val scanAllSpec = suite("scanAll")(
    test("without filter") {
      withSingleIdKeyTable { tableName =>
        for {
          _      <- put(tableName, Person("1", "Smith", Some("John"), 21)).execute
          _      <- put(tableName, Person("2", "Brown", None, 42)).execute
          stream <- scanAll[Person](tableName).execute
          people <- stream.runCollect
        } yield assertTrue(people == Chunk(Person("1", "Smith", Some("John"), 21), Person("2", "Brown", None, 42)))
      }
    },
    test("with parrallel server side scan") {
      withSingleIdKeyTable { tableName =>
        for {
          _      <- put(tableName, Person("1", "Smith", Some("John"), 21)).execute
          _      <- put(tableName, Person("2", "Brown", None, 42)).execute
          stream <- scanAll[Person](tableName).parallel(2).execute
          people <- stream.runCollect
        } yield assertTrue(
          people.sortBy(_.id) == Chunk(Person("1", "Smith", Some("John"), 21), Person("2", "Brown", None, 42))
            .sortBy(_.id) // parallel scan order is not guaranteed
        )
      }
    },
    test("with filter on forename exists") {
      withSingleIdKeyTable { tableName =>
        for {
          _      <- put(tableName, Person("1", "Smith", Some("John"), 21)).execute
          _      <- put(tableName, Person("2", "Brown", None, 42)).execute
          stream <- scanAll[Person](tableName).filter(Person.forename.exists).execute
          people <- stream.runCollect
        } yield assertTrue(people == Chunk(Person("1", "Smith", Some("John"), 21)))
      }
    },
    test("with filter id in ('1', '2') and age >= 21") {
      withSingleIdKeyTable { tableName =>
        for {
          _      <- put(tableName, Person("1", "Smith", Some("John"), 21)).execute
          _      <- put(tableName, Person("2", "Brown", None, 42)).execute
          stream <- scanAll[Person](tableName).filter(Person.id.in("1", "2") && Person.age > 21).execute
          people <- stream.runCollect
        } yield assertTrue(people == Chunk(Person("2", "Brown", None, 42)))
      }
    }
  )

  private val scanSomeSpec = suite("scanSome")(
    test("without filter pages until lastEvaluatedKey is empty") {
      withSingleIdKeyTable { tableName =>
        for {
          _                         <- put(tableName, Person("1", "Smith", Some("John"), 21)).execute
          _                         <- put(tableName, Person("2", "Brown", None, 42)).execute
          t                         <- scanSome[Person](tableName, 1).execute
          (page1, lastEvaluatedKey1) = t
          t2                        <- scanSome[Person](tableName, 1).startKey(lastEvaluatedKey1).execute
          (page2, lastEvaluatedKey2) = t2
          t3                        <- scanSome[Person](tableName, 1).startKey(lastEvaluatedKey2).execute
          (page3, lastEvaluatedKey3) = t3
        } yield assertTrue(page1 == Chunk(Person("1", "Smith", Some("John"), 21))) &&
          assertTrue(page2 == Chunk(Person("2", "Brown", None, 42))) &&
          assertTrue(page3.isEmpty) &&
          assertTrue(lastEvaluatedKey1.isDefined) &&
          assertTrue(lastEvaluatedKey2.isDefined) &&
          assertTrue(lastEvaluatedKey3.isEmpty)
      }
    },
    test("with filter pages until items Chunk is empty") {
      withSingleIdKeyTable { tableName =>
        val scanSomeWithFilter = scanSome[Person](tableName, 1).filter(Person.forename.exists)
        for {
          _                               <- put(tableName, Person("1", "Smith", Some("John"), 21)).execute
          _                               <- put(tableName, Person("2", "Brown", None, 42)).execute
          t                               <- scanSomeWithFilter.execute
          (peopleScan1, lastEvaluatedKey1) = t
          t2                              <- scanSomeWithFilter.startKey(lastEvaluatedKey1).execute
          (peopleScan2, lastEvaluatedKey2) = t2
        } yield assertTrue(peopleScan1 == Chunk(Person("1", "Smith", Some("John"), 21))) &&
          assertTrue(peopleScan2.isEmpty) &&
          assertTrue(lastEvaluatedKey1.isDefined) &&
          assertTrue(peopleScan2.isEmpty) &&
          assertTrue(lastEvaluatedKey2.isDefined)
      // note lastEvaluatedKey2 is present as item is still read by DynamoDB
      }
    }
  )

  final case class Equipment(id: String, year: String, name: String, price: Double)
  object Equipment {
    implicit val schema: Schema.CaseClass4[String, String, String, Double, Equipment] = DeriveSchema.gen[Equipment]
    val (id, year, name, price)                                                       = ProjectionExpression.accessors[Equipment]
  }

  private val queryAllSpec = suite("queryAll")(
    test("with only partition key expression") {
      withIdAndYearKeyTable { tableName =>
        for {
          _          <- put(tableName, Equipment("1", "2020", "Widget1", 1.0)).execute
          _          <- put(tableName, Equipment("1", "2021", "Widget1", 2.0)).execute
          stream     <- queryAll[Equipment](tableName)
                          .whereKey(Equipment.id.partitionKey === "1")
                          .execute
          equipments <- stream.runCollect
        } yield assertTrue(
          equipments == Chunk(Equipment("1", "2020", "Widget1", 1.0), Equipment("1", "2021", "Widget1", 2.0))
        )
      }
    },
    test("with partition key and sort key equality expression") {
      withIdAndYearKeyTable { tableName =>
        for {
          _          <- put(tableName, Equipment("1", "2020", "Widget1", 1.0)).execute
          _          <- put(tableName, Equipment("1", "2021", "Widget1", 2.0)).execute
          stream     <- queryAll[Equipment](tableName)
                          .whereKey(Equipment.id.partitionKey === "1" && Equipment.year.sortKey === "2020")
                          .execute
          equipments <- stream.runCollect
        } yield assertTrue(equipments == Chunk(Equipment("1", "2020", "Widget1", 1.0)))
      }
    },
    test("with partition key and sort key greater than expression") {
      withIdAndYearKeyTable { tableName =>
        for {
          _          <- put(tableName, Equipment("1", "2019", "Widget1", 1.0)).execute
          _          <- put(tableName, Equipment("1", "2020", "Widget1", 1.0)).execute
          _          <- put(tableName, Equipment("1", "2021", "Widget1", 2.0)).execute
          stream     <- queryAll[Equipment](tableName)
                          .whereKey(Equipment.id.partitionKey === "1" && Equipment.year.sortKey > "2019")
                          .execute
          equipments <- stream.runCollect
        } yield assertTrue(
          equipments == Chunk(Equipment("1", "2020", "Widget1", 1.0), Equipment("1", "2021", "Widget1", 2.0))
        )
      }
    },
    test("with partition key and sort key begins with expression") {
      withIdAndYearKeyTable { tableName =>
        for {
          _          <- put(tableName, Equipment("1", "1999", "Widget1", 1.0)).execute
          _          <- put(tableName, Equipment("1", "2020", "Widget1", 1.0)).execute
          _          <- put(tableName, Equipment("1", "2021", "Widget1", 2.0)).execute
          stream     <- queryAll[Equipment](tableName)
                          .whereKey(Equipment.id.partitionKey === "1" && Equipment.year.sortKey.beginsWith("20"))
                          .execute
          equipments <- stream.runCollect
        } yield assertTrue(
          equipments == Chunk(Equipment("1", "2020", "Widget1", 1.0), Equipment("1", "2021", "Widget1", 2.0))
        )
      }
    },
    test("with partition key and sort key between expression which is inclusive of min and max values") {
      withIdAndYearKeyTable { tableName =>
        for {
          _          <- put(tableName, Equipment("1", "1999", "Widget1", 1.0)).execute
          _          <- put(tableName, Equipment("1", "2020", "Widget1", 1.0)).execute
          _          <- put(tableName, Equipment("1", "2021", "Widget1", 2.0)).execute
          stream     <- queryAll[Equipment](tableName)
                          .whereKey(Equipment.id.partitionKey === "1" && Equipment.year.sortKey.between("2020", "2021"))
                          .execute
          equipments <- stream.runCollect
        } yield assertTrue(
          equipments == Chunk(Equipment("1", "2020", "Widget1", 1.0), Equipment("1", "2021", "Widget1", 2.0))
        )
      }
    }
  )

  private val querySomeSpec = suite("querySome")(
    test("with partion key pages until lastEvaluatedKey is empty") {
      withIdAndYearKeyTable { tableName =>
        val querySomeWithPartitionKey = querySome[Equipment](tableName, 1).whereKey(Equipment.id.partitionKey === "1")
        for {
          _                         <- put(tableName, Equipment("1", "2020", "Widget1", 1.0)).execute
          _                         <- put(tableName, Equipment("1", "2021", "Widget1", 2.0)).execute
          t                         <- querySomeWithPartitionKey.execute
          (page1, lastEvaluatedKey1) = t
          t2                        <- querySomeWithPartitionKey.startKey(lastEvaluatedKey1).execute
          (page2, lastEvaluatedKey2) = t2
          t3                        <- querySomeWithPartitionKey.startKey(lastEvaluatedKey2).execute
          (page3, lastEvaluatedKey3) = t3
        } yield assertTrue(page1 == Chunk(Equipment("1", "2020", "Widget1", 1.0))) &&
          assertTrue(page2 == Chunk(Equipment("1", "2021", "Widget1", 2.0))) &&
          assertTrue(page3.isEmpty) &&
          assertTrue(lastEvaluatedKey1.isDefined) &&
          assertTrue(lastEvaluatedKey2.isDefined) &&
          assertTrue(lastEvaluatedKey3.isEmpty)
      }
    },
    test("with partion key and sort key pages until lastEvaluatedKey is empty") {
      withIdAndYearKeyTable { tableName =>
        val query = querySome[Equipment](tableName, 1).whereKey(
          Equipment.id.partitionKey === "1" && Equipment.year.sortKey === "2020"
        )
        for {
          _                         <- put(tableName, Equipment("1", "2020", "Widget1", 1.0)).execute
          _                         <- put(tableName, Equipment("1", "2021", "Widget1", 2.0)).execute
          t                         <- query.execute
          (page1, lastEvaluatedKey1) = t
          t2                        <- query.startKey(lastEvaluatedKey1).execute
          (page2, lastEvaluatedKey2) = t2
        } yield assertTrue(page1 == Chunk(Equipment("1", "2020", "Widget1", 1.0))) &&
          assertTrue(page2.isEmpty) &&
          assertTrue(lastEvaluatedKey1.isDefined) &&
          assertTrue(lastEvaluatedKey2.isEmpty)
      }
    }
  )

}
