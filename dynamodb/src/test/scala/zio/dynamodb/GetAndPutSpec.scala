package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ get, put }
import zio.schema.{ DeriveSchema, Schema }
import zio.test.{ assertTrue, DefaultRunnableSpec }

object GetAndPutSpec extends DefaultRunnableSpec {
  final case class SimpleCaseClass2(id: Int, name: String)
  implicit lazy val simpleCaseClass2: Schema[SimpleCaseClass2] = DeriveSchema.gen[SimpleCaseClass2]

  val primaryKey1 = PrimaryKey("id" -> 1)
  val primaryKey2 = PrimaryKey("id" -> 2)

  override def spec = suite("get and put suite")(getSuite, putSuite).provideLayer(DynamoDBExecutor.test)

  val getSuite = suite("get item as SimpleCaseClass2")(
    testM("that exists") {
      for {
        _     <- TestDynamoDBExecutor.addTable("table1", "id", primaryKey1 -> Item("id" -> 1, "name" -> "Avi"))
        found <- get[SimpleCaseClass2]("table1", primaryKey1).execute
      } yield assertTrue(found == Right(SimpleCaseClass2(1, "Avi")))
    },
    testM("that does not exists") {
      for {
        _     <- TestDynamoDBExecutor.addTable("table1", "id")
        found <- get[SimpleCaseClass2]("table1", primaryKey1).execute
      } yield assertTrue(found == Left("value with key AttrMap(Map(id -> Number(1))) not found"))
    },
    testM("with missing attributes results in an error") {
      for {
        _     <- TestDynamoDBExecutor.addTable("table1", "id", primaryKey1 -> Item("id" -> 1))
        found <- get[SimpleCaseClass2]("table1", primaryKey1).execute
      } yield assertTrue(found == Left("field 'name' not found in Map(Map(String(id) -> Number(1)))"))
    },
    testM("batched") {
      for {
        _                <- TestDynamoDBExecutor.addTable(
                              "table1",
                              "id",
                              primaryKey1 -> Item("id" -> 1, "name" -> "Avi"),
                              primaryKey2 -> Item("id" -> 2, "name" -> "Tarlochan")
                            )
        (found1, found2) <-
          (get[SimpleCaseClass2]("table1", primaryKey1) zip get[SimpleCaseClass2]("table1", primaryKey2)).execute
      } yield assertTrue(found1 == Right(SimpleCaseClass2(1, "Avi"))) && assertTrue(
        found2 == Right(SimpleCaseClass2(2, "Tarlochan"))
      )
    }
  )

  val putSuite = suite("put")(
    testM("""SimpleCaseClass2(1, "Avi")""") {
      for {
        _     <- TestDynamoDBExecutor.addTable("table1", "id")
        _     <- put[SimpleCaseClass2]("table1", SimpleCaseClass2(1, "Avi")).execute
        found <- get[SimpleCaseClass2]("table1", primaryKey1).execute
      } yield assertTrue(found == Right(SimpleCaseClass2(1, "Avi")))
    },
    testM("""batched SimpleCaseClass2(1, "Avi") and SimpleCaseClass2(2, "Tarlochan")""") {
      for {
        _      <- TestDynamoDBExecutor.addTable("table1", "id")
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
