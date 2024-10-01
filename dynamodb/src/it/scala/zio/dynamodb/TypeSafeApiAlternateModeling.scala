package zio.dynamodb

import zio.Scope
import zio.test.Spec
import zio.test.{ assertTrue }
import zio.test.TestEnvironment
import zio.test.TestAspect
import zio.schema.annotation.discriminatorName
import zio.schema.Schema
import zio.schema.DeriveSchema

// An example of using a model that uses only sum and product types
object TypeSafeApiAlternateModeling extends DynamoDBLocalSpec {

  object dynamo {

    @discriminatorName("groupType")
    sealed trait GroupBody
    object GroupBody {
      final case class Abc(sku: String, amount: Int) extends GroupBody
      object Abc {
        implicit val schema: Schema.CaseClass2[String, Int, Abc] = DeriveSchema.gen[Abc]
        val (sku, amount)                                        = ProjectionExpression.accessors[Abc]
      }
      final case class Xyz(sku: String, otp: String) extends GroupBody
      object Xyz {
        implicit val schema: Schema.CaseClass2[String, String, Xyz] = DeriveSchema.gen[Xyz]
        val (sku, otp)                                              = ProjectionExpression.accessors[Xyz]
      }
      implicit val schema: Schema.Enum2[Abc, Xyz, GroupBody] = DeriveSchema.gen[GroupBody]
      val (fixed, otp) = ProjectionExpression.accessors[GroupBody]
    }

    @discriminatorName("invoiceType")
    sealed trait ContractBody
    object ContractBody {
      final case class Simple(sku: String) extends ContractBody // simple types just have fields
      object Simple {
        implicit val schema: Schema.CaseClass1[String, Simple] = DeriveSchema.gen[Simple]
        val sku                                                = ProjectionExpression.accessors[Simple]
      }
      final case class Group(products: Map[String, String], body: GroupBody) extends ContractBody
      object Group  {
        implicit val schema: Schema.CaseClass2[Map[String, String], GroupBody, Group] = DeriveSchema.gen[Group]
        val (products, body)                                                          = ProjectionExpression.accessors[Group]
      }
      implicit val schema: Schema.Enum2[Simple, Group, ContractBody] = DeriveSchema.gen[ContractBody]
      val (ops, direct) = ProjectionExpression.accessors[ContractBody]
    }

    final case class Contract(
      id: String,
      alternateId: Option[String], // secondary keys have to be scalar and at top level
      accountId: Option[String],   // secondary keys have to be scalar and at top level
      isTest: Boolean,
      body: ContractBody
    )               {
      def contractType: String =
        body match {
          case ContractBody.Simple(_)                     => "simple"
          case ContractBody.Group(_, GroupBody.Abc(_, _)) => "group:abc"
          case ContractBody.Group(_, GroupBody.Xyz(_, _)) => "group:xyz"
        }
    }
    object Contract {
      implicit val schema: Schema.CaseClass5[String, Option[String], Option[String], Boolean, ContractBody, Contract] =
        DeriveSchema.gen[Contract]
      val (id, identityId, accountId, isTest, body)                                                                   = ProjectionExpression.accessors[Contract]
    }

  }

  val modelExperimentSuite                                                  = suite("alternate model")(
    test("crud operations") {
      withIdAndAccountIdGsiTable { invoiceTable =>
        import dynamo._
        println(invoiceTable)
        val abc          = Contract(
          "1",
          alternateId = None,
          accountId = Some("a1"),
          true,
          ContractBody.Group(Map("a" -> "b"), GroupBody.Abc("sku", 1))
        )
        val accountIdKey = Contract.accountId.partitionKey === Some("a1")
        for {
          _        <- DynamoDBQuery
                        .put(invoiceTable, abc)
                        .where(!Contract.id.exists) // expressions operate on top level fields
                        .execute
          contract <- DynamoDBQuery.get(invoiceTable)(Contract.id.partitionKey === "1").execute.absolve
          stream   <- DynamoDBQuery.queryAll[Contract](invoiceTable).indexName("accountId").whereKey(accountIdKey).execute
          xs       <- stream.runCollect
        } yield assertTrue(contract.contractType == "group:abc" && xs.size == 1)

      }
    }
  )
  override def spec: Spec[Environment with TestEnvironment with Scope, Any] =
    suite("all")(
      modelExperimentSuite
    ) @@ TestAspect.nondeterministic

}
