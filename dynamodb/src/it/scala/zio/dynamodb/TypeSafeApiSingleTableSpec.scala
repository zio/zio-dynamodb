package zio.dynamodb

import zio.test.assertTrue
import zio.test.TestAspect
import zio.test.Spec
import zio.test.TestEnvironment
import zio.Scope
import zio.schema.Schema
import zio.schema.DeriveSchema
import java.time.Instant
import zio.schema.annotation.discriminatorName
import zio.ZIO

/**
 * Demonstrates how to implement the single table pattern (also known as the adjacency lists pattern) using the type-safe API
 * using sum and product types.
 * see https://www.alexdebrie.com/posts/dynamodb-single-table/
 * see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-adjacency-graphs.html
 */
object TypeSafeApiSingleTableSpec extends DynamoDBLocalSpec {

  @discriminatorName("userBodyType")
  sealed trait UserBody
  object UserBody {

    final case class Profile private (username: String, fullName: String, email: String, createdAt: Instant)
        extends UserBody
    object Profile {
      implicit val schema: Schema.CaseClass4[String, String, String, Instant, Profile] = DeriveSchema.gen[Profile]
      val (username, fullName, email, createdAt)                                       = ProjectionExpression.accessors[Profile]
    }

    final case class Order private (username: String, orderId: String, status: String, createdAt: Instant)
        extends UserBody
    object Order {
      implicit val schema: Schema.CaseClass4[String, String, String, Instant, Order] = DeriveSchema.gen[Order]
      val (userName, orderId, status, createdAt)                                     = ProjectionExpression.accessors[Order]
    }

    implicit val schema: Schema.Enum2[Profile, Order, UserBody] = DeriveSchema.gen[UserBody]
    val (profile, order)                                        = ProjectionExpression.accessors[UserBody]
  }
  final case class User private (id: String, selector: String, userBody: UserBody)
  object User {
    implicit val schema: Schema.CaseClass3[String, String, UserBody, User] = DeriveSchema.gen[User]
    val (id, selector, userBody)                                           = ProjectionExpression.accessors[User]

    // smart constructors manage the id and sort keys

    def makeProfile(username: String, fullName: String, email: String, createdAt: Instant): User =
      User(s"USER:$username", s"Profile:$username", UserBody.Profile(username, fullName, email, createdAt))

    def makeOrder(username: String, orderId: String, status: String, createdAt: Instant): User =
      User(s"USER:$username", s"Order:$orderId", UserBody.Order(username, orderId, status, createdAt))
  }
  override def spec: Spec[Environment with TestEnvironment with Scope, Any] =
    suite("suite")(
      test("test") {
        withIdAndSelectorKeyTable { tableName =>
          for {
            now    <- zio.Clock.instant
            _      <- DynamoDBQuery.put(tableName, User.makeProfile("Bob", "Bob Smith", "bob@gmail.com", now)).execute
            _      <- DynamoDBQuery.put(tableName, User.makeOrder("Bob", "123", "pending", now)).execute
            _      <- DynamoDBQuery.put(tableName, User.makeOrder("Bob", "124", "pending", now)).execute
            stream <- DynamoDBQuery
                        .queryAll[User](tableName)
                        .whereKey(
                          User.id.partitionKey === "USER:Bob" && User.selector.sortKey.beginsWith("Order")
                        )
                        .execute
            _      <- stream.tap(ZIO.debug(_)).runDrain
            // User(USER:Bob,Order#123,Order(Bob,123,pending,1970-01-01T00:00:00Z))
            // User(USER:Bob,Order#124,Order(Bob,124,pending,1970-01-01T00:00:00Z))
          } yield assertTrue(true)
        }

      }
    ) @@ TestAspect.nondeterministic
}
