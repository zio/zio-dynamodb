package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ get, getItem, put, scanAll }
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
      topLevelSumTypeSuite,
      nestedSumTypeSuite
    ) @@ TestAspect.nondeterministic

  val topLevelSumTypeDiscriminatorNameSuite = suite("with @discriminatorName annotation")(
    test("put of a concrete sub type wil not generate a discriminator - so don't do this!") {
      withSingleIdKeyTable { invoiceTable =>
        for {
          _       <- put[InvoiceWithNoDiscriminator.Unpaid](invoiceTable, InvoiceWithNoDiscriminator.Unpaid("1")).execute
          invoice <- getItem(invoiceTable, PrimaryKey("id" -> "1")).execute
        } yield assertTrue(invoice == Some(Item("id" -> "1")))
      }
    },
    test("put of a top level sum type will generate a discriminator field") {
      withSingleIdKeyTable { invoiceTable =>
        val key     = InvoiceWithDiscriminatorName.unpaid >>> InvoiceWithDiscriminatorName.Unpaid.id
        val keyCond = key.partitionKey === "1"
        for {
          _       <- put[InvoiceWithDiscriminatorName](invoiceTable, InvoiceWithDiscriminatorName.Unpaid("1")).execute
          invoice <- get[InvoiceWithDiscriminatorName](invoiceTable)(keyCond).execute.absolve
          item    <- getItem(invoiceTable, PrimaryKey("id" -> "1")).execute
        } yield assertTrue(
          invoice == InvoiceWithDiscriminatorName.Unpaid("1") && item == Some(
            Item("id" -> "1", "invoiceType" -> "Unpaid")
          )
        )
      }
    },
    test("scanAll") {
      withSingleIdKeyTable { invoiceTable =>
        for {
          _        <- put[InvoiceWithDiscriminatorName](invoiceTable, InvoiceWithDiscriminatorName.Unpaid("UNPAID:1")).execute
          _        <- put[InvoiceWithDiscriminatorName](invoiceTable, InvoiceWithDiscriminatorName.Paid("PAID:1", 100)).execute
          stream   <- scanAll[InvoiceWithDiscriminatorName](invoiceTable).execute
          invoices <- stream.runCollect
        } yield (assertTrue(
          invoices.sortBy(_.id) == Chunk(
            InvoiceWithDiscriminatorName.Paid("PAID:1", 100),
            InvoiceWithDiscriminatorName.Unpaid("UNPAID:1")
          )
        ))
      }
    },
    test("scanAll with filter using optics on Paid sum type") {
      withSingleIdKeyTable { invoiceTable =>
        for {
          _        <- put[InvoiceWithDiscriminatorName](invoiceTable, InvoiceWithDiscriminatorName.Unpaid("UNPAID:1")).execute
          _        <- put[InvoiceWithDiscriminatorName](invoiceTable, InvoiceWithDiscriminatorName.Paid("PAID:1", 40)).execute
          _        <- put[InvoiceWithDiscriminatorName](invoiceTable, InvoiceWithDiscriminatorName.Paid("PAID:2", 100)).execute
          stream   <-
            scanAll[InvoiceWithDiscriminatorName](invoiceTable)
              .filter(
                (InvoiceWithDiscriminatorName.paid >>> InvoiceWithDiscriminatorName.Paid.id beginsWith "PAID") &&
                  InvoiceWithDiscriminatorName.paid >>> InvoiceWithDiscriminatorName.Paid.amount > 50
              )
              .execute
          invoices <- stream.runCollect
        } yield (assertTrue(
          invoices.sortBy(_.id) == Chunk(
            InvoiceWithDiscriminatorName.Paid("PAID:2", 100)
          )
        ))
      }
    }
  )

  val topLevelSumTypeNoDiscriminatorSuite = suite("with @noDiscriminator annotation")(
    test("put and get top level sum type") {
      withSingleIdKeyTable { invoiceTable =>
        val key     = InvoiceWithNoDiscriminator.unpaid >>> InvoiceWithNoDiscriminator.Unpaid.id
        val keyCond = key.partitionKey === "1"
        for {
          _       <- put(invoiceTable, InvoiceWithNoDiscriminator.Unpaid("1")).execute
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

  val topLevelSumTypeSuite =
    suite("Top level sum type suite")(topLevelSumTypeDiscriminatorNameSuite, topLevelSumTypeNoDiscriminatorSuite)

  sealed trait AbodeType
  object AbodeType {
    case object House extends AbodeType
    case object Flat  extends AbodeType

    implicit val schema: Schema.Enum2[House.type, Flat.type, AbodeType] = DeriveSchema.gen[AbodeType]
    val (house, flat)                                                   = ProjectionExpression.accessors[AbodeType]
  }

  sealed trait AddressDefaultMapping {
    def abodeType: AbodeType
  }
  object AddressDefaultMapping       {
    final case class UKAddress(postcode: String, abodeType: AbodeType) extends AddressDefaultMapping
    object UKAddress {
      implicit val schema: Schema.CaseClass2[String, AbodeType, UKAddress] = DeriveSchema.gen[UKAddress]
      val (postcode, abodeType)                                            = ProjectionExpression.accessors[UKAddress]
    }
    final case class USAddress(zip: String, abodeType: AbodeType) extends AddressDefaultMapping
    object USAddress {
      implicit val schema: Schema.CaseClass2[String, AbodeType, USAddress] = DeriveSchema.gen[USAddress]
      val (zip, abodeType)                                                 = ProjectionExpression.accessors[USAddress]
    }
    implicit val schema: Schema.Enum2[UKAddress, USAddress, AddressDefaultMapping] =
      DeriveSchema.gen[AddressDefaultMapping]
    val (ukAddress, usAddress) = ProjectionExpression.accessors[AddressDefaultMapping]
  }

  final case class PersonDefault(id: String, surname: String, address: AddressDefaultMapping)
  object PersonDefault {
    implicit val schema: Schema.CaseClass3[String, String, AddressDefaultMapping, PersonDefault] =
      DeriveSchema.gen[PersonDefault]
    val (id, surname, address)                                                                   = ProjectionExpression.accessors[PersonDefault]
  }

  private val nestedSumTypeDefaultMappingSuite = suite("with default mapping")(
    test("put and get person with UK address") {
      val ukPerson = PersonDefault("1", "Smith", AddressDefaultMapping.UKAddress("XY99 7QW", AbodeType.House))
      withSingleIdKeyTable { personTable =>
        for {
          _      <- put(personTable, ukPerson).execute
          person <- get(personTable)(PersonDefault.id.partitionKey === "1").execute.absolve
        } yield assertTrue(person == ukPerson)
      }
    },
    test("put and get person with UK address") {
      val usPerson = PersonDefault("1", "Smith", AddressDefaultMapping.USAddress("12345", AbodeType.House))
      withSingleIdKeyTable { personTable =>
        for {
          _      <- put(personTable, usPerson).execute
          person <- get(personTable)(PersonDefault.id.partitionKey === "1").execute.absolve
        } yield assertTrue(person == usPerson)
      }
    }
  )

  @discriminatorName("addressType")
  sealed trait AddressDiscriminatorName {
    def abodeType: AbodeType
  }

  object AddressDiscriminatorName {
    final case class UKAddress(postcode: String, abodeType: AbodeType) extends AddressDiscriminatorName
    object UKAddress {
      implicit val schema: Schema.CaseClass2[String, AbodeType, UKAddress] = DeriveSchema.gen[UKAddress]
      val (postcode, abodeType)                                            = ProjectionExpression.accessors[UKAddress]
    }
    final case class USAddress(zip: String, abodeType: AbodeType) extends AddressDiscriminatorName
    object USAddress {
      implicit val schema: Schema.CaseClass2[String, AbodeType, USAddress] = DeriveSchema.gen[USAddress]
      val (zip, abodeType)                                                 = ProjectionExpression.accessors[USAddress]
    }
    implicit val schema: Schema.Enum2[UKAddress, USAddress, AddressDiscriminatorName] =
      DeriveSchema.gen[AddressDiscriminatorName]
    val (ukAddress, usAddress) = ProjectionExpression.accessors[AddressDiscriminatorName]
  }

  final case class PersonDiscriminatorName(id: String, surname: String, address: AddressDiscriminatorName)
  object PersonDiscriminatorName {
    implicit val schema: Schema.CaseClass3[String, String, AddressDiscriminatorName, PersonDiscriminatorName] =
      DeriveSchema.gen[PersonDiscriminatorName]
    val (id, surname, address)                                                                                = ProjectionExpression.accessors[PersonDiscriminatorName]
  }

  private val nestedSumTypeDisriminatorNameMappingSuite = suite("with @disriminatorName annotation")(
    test("put and get person with UK address") {
      val ukPerson =
        PersonDiscriminatorName("1", "Smith", AddressDiscriminatorName.UKAddress("XY99 7QW", AbodeType.House))
      withSingleIdKeyTable { personTable =>
        for {
          _      <- put(personTable, ukPerson).execute
          person <- get(personTable)(PersonDiscriminatorName.id.partitionKey === "1").execute.absolve
        } yield assertTrue(person == ukPerson)
      }
    },
    test("put and get person with US address") {
      val usPerson = PersonDiscriminatorName("1", "Smith", AddressDiscriminatorName.USAddress("12345", AbodeType.House))
      withSingleIdKeyTable { personTable =>
        for {
          _      <- put(personTable, usPerson).execute
          person <- get(personTable)(PersonDiscriminatorName.id.partitionKey === "1").execute.absolve
        } yield assertTrue(person == usPerson)
      }
    }
  )

  // WARNING - this annotation is only meant for integrating with legacy schemas - do not use green field work!
  @noDiscriminator
  sealed trait AddressNoDiscriminator {
    def abodeType: AbodeType
  }
  object AddressNoDiscriminator       {
    final case class UKAddress(postcode: String, abodeType: AbodeType) extends AddressNoDiscriminator
    object UKAddress {
      implicit val schema: Schema.CaseClass2[String, AbodeType, UKAddress] = DeriveSchema.gen[UKAddress]
      val postcode                                                         = ProjectionExpression.accessors[UKAddress]
    }
    final case class USAddress(zip: String, abodeType: AbodeType) extends AddressNoDiscriminator
    object USAddress {
      implicit val schema: Schema.CaseClass2[String, AbodeType, USAddress] = DeriveSchema.gen[USAddress]
      val zip                                                              = ProjectionExpression.accessors[USAddress]
    }
    implicit val schema: Schema.Enum2[UKAddress, USAddress, AddressNoDiscriminator] =
      DeriveSchema.gen[AddressNoDiscriminator]
    val (ukAddress, usAddress) = ProjectionExpression.accessors[AddressNoDiscriminator]
  }

  final case class PersonNoDiscriminator(id: String, surname: String, address: AddressNoDiscriminator)
  object PersonNoDiscriminator {
    implicit val schema: Schema.CaseClass3[String, String, AddressNoDiscriminator, PersonNoDiscriminator] =
      DeriveSchema.gen[PersonNoDiscriminator]
    val (id, surname, address)                                                                            = ProjectionExpression.accessors[PersonNoDiscriminator]
  }

  private val nestedSumTypeNoDisriminatorNameMappingSuite = suite("with @noDiscrimator annotation")(
    test("put and get person with UK address") {
      val ukPerson =
        PersonNoDiscriminator("1", "Smith", AddressNoDiscriminator.UKAddress("XY99 7QW", AbodeType.House))
      withSingleIdKeyTable { personTable =>
        for {
          _      <- put(personTable, ukPerson).execute
          person <- get(personTable)(PersonNoDiscriminator.id.partitionKey === "1").execute.absolve
        } yield assertTrue(person == ukPerson)
      }
    },
    test("put and get person with US address") {
      val usPerson = PersonNoDiscriminator("1", "Smith", AddressNoDiscriminator.USAddress("12345", AbodeType.House))
      withSingleIdKeyTable { personTable =>
        for {
          _      <- put(personTable, usPerson).execute
          person <- get(personTable)(PersonNoDiscriminator.id.partitionKey === "1").execute.absolve
        } yield assertTrue(person == usPerson)
      }
    }
  )

  private val nestedSumTypeSuite = suite("Nested sum type suite")(
    nestedSumTypeDefaultMappingSuite,
    nestedSumTypeDisriminatorNameMappingSuite,
    nestedSumTypeNoDisriminatorNameMappingSuite
  )
}
