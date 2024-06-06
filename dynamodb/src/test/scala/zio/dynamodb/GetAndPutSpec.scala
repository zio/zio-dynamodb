package zio.dynamodb

import zio.dynamodb.DynamoDBError.ItemError.{ DecodingError, ValueNotFound }
import zio.dynamodb.DynamoDBQuery.{ get, put }
import zio.dynamodb.codec.Invoice
import zio.dynamodb.codec.Invoice.PreBilled
import zio.schema.{ DeriveSchema, Schema }
import zio.test.{ assertTrue, ZIOSpecDefault }
import zio.test.Spec

object GetAndPutSpec extends ZIOSpecDefault {
  final case class SimpleCaseClass2(id: Int, name: String)
  object SimpleCaseClass2 {
    implicit val schema: Schema.CaseClass2[Int, String, SimpleCaseClass2] = DeriveSchema.gen[SimpleCaseClass2]
    val (id, name)                                                        = ProjectionExpression.accessors[SimpleCaseClass2]
  }

  final case class SimpleCaseClass2OptionalField(id: Int, maybeName: Option[String])
  implicit val simpleCaseClass2: Schema[SimpleCaseClass2]                           = DeriveSchema.gen[SimpleCaseClass2]
  implicit val simpleCaseClass2OptionalField: Schema[SimpleCaseClass2OptionalField] =
    DeriveSchema.gen[SimpleCaseClass2OptionalField]

  private val primaryKey1 = PrimaryKey("id" -> 1)
  private val primaryKey2 = PrimaryKey("id" -> 2)

  override def spec: Spec[Environment, Any]    =
    suite("get and put suite")(getSuite, putSuite).provideLayer(DynamoDBExecutor.test("table1" -> "id"))

  private val getSuite = suite("get item as SimpleCaseClass2")(
    test("that exists") {
      for {
        _     <- TestDynamoDBExecutor.addItems("table1", primaryKey1 -> Item("id" -> 1, "name" -> "Avi"))
        found <- get("table1")(SimpleCaseClass2.id.partitionKey === 1).execute
      } yield assertTrue(found == Right(SimpleCaseClass2(1, "Avi")))
    },
    test("that does not exists") {
      for {
        found <- get("table1")(SimpleCaseClass2.id.partitionKey === 1).execute
      } yield assertTrue(found == Left(ValueNotFound("value with key AttrMap(Map(id -> Number(1))) not found")))
    },
    test("with missing attributes results in an error") {
      for {
        _     <- TestDynamoDBExecutor.addItems("table1", primaryKey1 -> Item("id" -> 1))
        found <- get("table1")(SimpleCaseClass2.id.partitionKey === 1).execute
      } yield assertTrue(found == Left(DecodingError("field 'name' not found in Map(Map(String(id) -> Number(1)))")))
    },
    test("batched") {
      for {
        _ <- TestDynamoDBExecutor.addItems(
               "table1",
               primaryKey1 -> Item("id" -> 1, "name" -> "Avi"),
               primaryKey2 -> Item("id" -> 2, "name" -> "Tarlochan")
             )
        r <- (get("table1")(SimpleCaseClass2.id.partitionKey === 1) zip get("table1")(
                 SimpleCaseClass2.id.partitionKey === 2
               )).execute
      } yield assertTrue(r._1 == Right(SimpleCaseClass2(1, "Avi"))) && assertTrue(
        r._2 == Right(SimpleCaseClass2(2, "Tarlochan"))
      )
    }
  )

  private val putSuite = suite("put")(
    test("""SimpleCaseClass2(1, "Avi")""") {
      for {
        _     <- put[SimpleCaseClass2]("table1", SimpleCaseClass2(1, "Avi")).execute
        found <- get("table1")(SimpleCaseClass2.id.partitionKey === 1).execute
      } yield assertTrue(found == Right(SimpleCaseClass2(1, "Avi")))
    },
    test("""top level enum PreBilled(1, "foobar")""") {
      for {
        _     <- TestDynamoDBExecutor.addTable("table1", "id")
        _     <- put[Invoice]("table1", PreBilled(1, "foobar")).execute
        found <- get("table1")(PreBilled.id.partitionKey === 1).execute.absolve
      } yield assertTrue(found == PreBilled(1, "foobar"))
    },
    test("""batched SimpleCaseClass2(1, "Avi") and SimpleCaseClass2(2, "Tarlochan")""") {
      for {
        _      <- (put[SimpleCaseClass2]("table1", SimpleCaseClass2(1, "Avi")) zip put[SimpleCaseClass2](
                      "table1",
                      SimpleCaseClass2(2, "Tarlochan")
                    )).execute
        found1 <- get("table1")(SimpleCaseClass2.id.partitionKey === 1).execute
        found2 <- get("table1")(SimpleCaseClass2.id.partitionKey === 2).execute
      } yield assertTrue(found1 == Right(SimpleCaseClass2(1, "Avi"))) && assertTrue(
        found2 == Right(SimpleCaseClass2(2, "Tarlochan"))
      )
    }
  )

}
