package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ get, put, scanAll }
import zio.Scope
import zio.test.Spec
import zio.test.assertTrue
import zio.test.TestEnvironment
import zio.schema.Schema
import zio.schema.DeriveSchema
import zio.Chunk
import zio.schema.annotation.discriminatorName
import zio.schema.annotation.noDiscriminator
import zio.test.TestAspect

object TypeSafeApiMappingSpec extends DynamoDBLocalSpec {

  // note the default sum type mapping does not work with top level sum types as it required an intermediate map
  // and partition keys must be scalar values

  @discriminatorName("invoiceType")
  sealed trait InvoiceWithDiscriminatorName {
    def id: String
  }
  object InvoiceWithDiscriminatorName       {
    final case class Unpaid(id: String) extends InvoiceWithDiscriminatorName
    object Unpaid {
      implicit val schema: Schema.CaseClass1[String, Unpaid] = DeriveSchema.gen[Unpaid]
      val id                                                 = ProjectionExpression.accessors[Unpaid]
    }
    final case class Paid(id: String, amount: Int) extends InvoiceWithDiscriminatorName
    object Paid   {
      implicit val schema: Schema.CaseClass2[String, Int, Paid] = DeriveSchema.gen[Paid]
      val (id, amount)                                          = ProjectionExpression.accessors[Paid]
    }
    implicit val schema: Schema.Enum2[Unpaid, Paid, InvoiceWithDiscriminatorName] =
      DeriveSchema.gen[InvoiceWithDiscriminatorName]
    val (unpaid, paid) = ProjectionExpression.accessors[InvoiceWithDiscriminatorName]
  }

  // WARNING - this annotation is only meant for integrating with legacy schemas - do not use green field work!
  @noDiscriminator
  sealed trait InvoiceWithNoDiscriminator {
    def id: String
  }
  object InvoiceWithNoDiscriminator       {
    final case class Unpaid(id: String) extends InvoiceWithNoDiscriminator
    object Unpaid {
      implicit val schema: Schema.CaseClass1[String, Unpaid] = DeriveSchema.gen[Unpaid]
      val id                                                 = ProjectionExpression.accessors[Unpaid]
    }
    final case class Paid(id: String, amount: Int) extends InvoiceWithNoDiscriminator
    object Paid   {
      implicit val schema: Schema.CaseClass2[String, Int, Paid] = DeriveSchema.gen[Paid]
      val (id, amount)                                          = ProjectionExpression.accessors[Paid]
    }
    implicit val schema: Schema.Enum2[Unpaid, Paid, InvoiceWithNoDiscriminator] =
      DeriveSchema.gen[InvoiceWithNoDiscriminator]
    val (unpaid, paid) = ProjectionExpression.accessors[InvoiceWithNoDiscriminator]
  }

  override def spec: Spec[Environment with TestEnvironment with Scope, Any] =
    suite("all")(
      topLevelSumTypeDiscriminatorNameSuite,
      topLevelSumTypeNoDiscriminatorSuite
    ) @@ TestAspect.nondeterministic

  val topLevelSumTypeDiscriminatorNameSuite = suite("top level sum type with discriminatorName")(
    test("put and get concrete sub type") {
      withSingleIdKeyTable { invoiceTable =>
        for {
          _       <- put(invoiceTable, InvoiceWithDiscriminatorName.Unpaid("1")).execute
          invoice <- get(invoiceTable)(InvoiceWithDiscriminatorName.Unpaid.id.partitionKey === "1").execute.absolve
        } yield assertTrue(invoice == InvoiceWithDiscriminatorName.Unpaid("1"))
      }
    },
    test("put and get top level sum type") {
      withSingleIdKeyTable { invoiceTable =>
        val key     = InvoiceWithDiscriminatorName.unpaid >>> InvoiceWithDiscriminatorName.Unpaid.id
        val keyCond = key.partitionKey === "1"
        for {
          _       <- put[InvoiceWithDiscriminatorName](invoiceTable, InvoiceWithDiscriminatorName.Unpaid("1")).execute
          invoice <- get(invoiceTable)(keyCond).execute.absolve
        } yield assertTrue(invoice == InvoiceWithDiscriminatorName.Unpaid("1"))
      }
    },
    test("scanAll") {
      withSingleIdKeyTable { invoiceTable =>
        for {
          _        <- put[InvoiceWithDiscriminatorName](invoiceTable, InvoiceWithDiscriminatorName.Unpaid("1")).execute
          _        <- put[InvoiceWithDiscriminatorName](invoiceTable, InvoiceWithDiscriminatorName.Paid("2", 100)).execute
          stream   <- scanAll[InvoiceWithDiscriminatorName](invoiceTable).execute
          invoices <- stream.runCollect
        } yield (assertTrue(
          invoices.sortBy(_.id) == Chunk(
            InvoiceWithDiscriminatorName.Unpaid("1"),
            InvoiceWithDiscriminatorName.Paid("2", 100)
          )
        ))
      }
    }
  )

  val topLevelSumTypeNoDiscriminatorSuite = suite("top level sum type with noDiscriminator")(
    test("put and get concrete sub type") {
      withSingleIdKeyTable { invoiceTable =>
        for {
          _       <- put(invoiceTable, InvoiceWithDiscriminatorName.Unpaid("1")).execute
          invoice <- get(invoiceTable)(InvoiceWithNoDiscriminator.Unpaid.id.partitionKey === "1").execute.absolve
        } yield assertTrue(invoice == InvoiceWithNoDiscriminator.Unpaid("1"))
      }
    },
    test("put and get top level sum type") {
      withSingleIdKeyTable { invoiceTable =>
        val key     = InvoiceWithNoDiscriminator.unpaid >>> InvoiceWithNoDiscriminator.Unpaid.id
        val keyCond = key.partitionKey === "1"
        for {
          _       <- put[InvoiceWithNoDiscriminator](invoiceTable, InvoiceWithNoDiscriminator.Unpaid("1")).execute
          invoice <- get(invoiceTable)(keyCond).execute.absolve
        } yield assertTrue(invoice == InvoiceWithNoDiscriminator.Unpaid("1"))
      }
    },
    test("scanAll") {
      withSingleIdKeyTable { invoiceTable =>
        for {
          _        <- put[InvoiceWithNoDiscriminator](invoiceTable, InvoiceWithNoDiscriminator.Unpaid("1")).execute
          _        <- put[InvoiceWithNoDiscriminator](invoiceTable, InvoiceWithNoDiscriminator.Paid("2", 100)).execute
          stream   <- scanAll[InvoiceWithNoDiscriminator](invoiceTable).execute
          invoices <- stream.runCollect
        } yield (assertTrue(
          invoices.sortBy(_.id) == Chunk(
            InvoiceWithNoDiscriminator.Unpaid("1"),
            InvoiceWithNoDiscriminator.Paid("2", 100)
          )
        ))
      }
    }
  )
}
