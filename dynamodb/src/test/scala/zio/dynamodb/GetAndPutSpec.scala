package zio.dynamodb

import zio.dynamodb.DynamoDBError.{DecodingError, ValueNotFound}
import zio.dynamodb.DynamoDBQuery.{get, put}
import zio.dynamodb.codec.Invoice
import zio.dynamodb.codec.Invoice.PreBilled
import zio.schema.{DeriveSchema, Schema}
import zio.test.{ZIOSpecDefault, assertTrue}

object GetAndPutSpec extends ZIOSpecDefault {
  final case class SimpleCaseClass2(id: Int, name: String)
  final case class SimpleCaseClass2OptionalField(id: Int, maybeName: Option[String])
  implicit lazy val simpleCaseClass2: Schema[SimpleCaseClass2]                           = DeriveSchema.gen[SimpleCaseClass2]
  implicit lazy val simpleCaseClass2OptionalField: Schema[SimpleCaseClass2OptionalField] =
    DeriveSchema.gen[SimpleCaseClass2OptionalField]

  private val primaryKey1 = PrimaryKey("id" -> 1)
  private val primaryKey2 = PrimaryKey("id" -> 2)

  override def spec    =
    suite("get and put suite")(getSuite, putSuite).provideLayer(DynamoDBExecutor.test("table1" -> "id"))

  private val getSuite = suite("get item as SimpleCaseClass2")(
    test("that exists") {
      for {
        _     <- TestDynamoDBExecutor.addItems("table1", primaryKey1 -> Item("id" -> 1, "name" -> "Avi"))
        found <- get[SimpleCaseClass2]("table1", primaryKey1).execute
      } yield assertTrue(found == Right(SimpleCaseClass2(1, "Avi")))
    },
    test("that does not exists") {
      for {
        found <- get[SimpleCaseClass2]("table1", primaryKey1).execute
      } yield assertTrue(found == Left(ValueNotFound("value with key AttrMap(Map(id -> Number(1))) not found")))
    },
    test("with missing attributes results in an error") {
      for {
        _     <- TestDynamoDBExecutor.addItems("table1", primaryKey1 -> Item("id" -> 1))
        found <- get[SimpleCaseClass2]("table1", primaryKey1).execute
      } yield assertTrue(found == Left(DecodingError("field 'name' not found in Map(Map(String(id) -> Number(1)))")))
    },
    test("batched") {
      for {
        _ <- TestDynamoDBExecutor.addItems(
               "table1",
               primaryKey1 -> Item("id" -> 1, "name" -> "Avi"),
               primaryKey2 -> Item("id" -> 2, "name" -> "Tarlochan")
             )
        r <- (get[SimpleCaseClass2]("table1", primaryKey1) zip get[SimpleCaseClass2]("table1", primaryKey2)).execute
      } yield assertTrue(r._1 == Right(SimpleCaseClass2(1, "Avi"))) && assertTrue(
        r._2 == Right(SimpleCaseClass2(2, "Tarlochan"))
      )
    }
  )

  private val putSuite = suite("put")(
    test("""SimpleCaseClass2(1, "Avi")""") {
      for {
        _     <- put[SimpleCaseClass2]("table1", SimpleCaseClass2(1, "Avi")).execute
        found <- get[SimpleCaseClass2]("table1", primaryKey1).execute
      } yield assertTrue(found == Right(SimpleCaseClass2(1, "Avi")))
    },
    test("""top level enum PreBilled(1, "foobar")""") {
      for {
        _     <- TestDynamoDBExecutor.addTable("table1", "id")
        _     <- put[Invoice]("table1", PreBilled(1, "foobar")).execute
        found <- get[Invoice]("table1", primaryKey1).execute
      } yield assertTrue(found == Right(PreBilled(1, "foobar")))
    },
    test("""batched SimpleCaseClass2(1, "Avi") and SimpleCaseClass2(2, "Tarlochan")""") {
      for {
        _      <- (put[SimpleCaseClass2]("table1", SimpleCaseClass2(1, "Avi")) zip put[SimpleCaseClass2](
                      "table1",
                      SimpleCaseClass2(2, "Tarlochan")
                    )).execute
        found1 <- get[SimpleCaseClass2]("table1", primaryKey1).execute
        found2 <- get[SimpleCaseClass2]("table1", primaryKey2).execute
      } yield assertTrue(found1 == Right(SimpleCaseClass2(1, "Avi"))) && assertTrue(
        found2 == Right(SimpleCaseClass2(2, "Tarlochan"))
      )
    }
  )

}
