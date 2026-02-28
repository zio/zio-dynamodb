package zio.dynamodb

import zio.Scope
import zio.test._

// Scala 3 enum to test version-specific source directory
enum Status:
  case Active
  case Inactive
  case Pending

// Simple case class using the enum
case class User(name: String, status: Status)

object Scala3EnumSpec extends ZIOSpecDefault:
  
  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("Scala 3 enum support test")(
      test("enum pattern matching with Scala 3 syntax") {
        val status: Status = Status.Active
        
        val result = status match
          case Status.Active   => "active"
          case Status.Inactive => "inactive"
          case Status.Pending  => "pending"
        
        assertTrue(result == "active")
      },
      
      test("enum values can be used in case classes") {
        val user = User("Alice", Status.Pending)
        
        assertTrue(
          user.name == "Alice" && 
          user.status == Status.Pending
        )
      },
      
      test("enum ordinal values") {
        assertTrue(
          Status.Active.ordinal == 0 &&
          Status.Inactive.ordinal == 1 &&
          Status.Pending.ordinal == 2
        )
      },
      
      test("enum valueOf") {
        val active = Status.valueOf("Active")
        assertTrue(active == Status.Active)
      },
      
      test("using Scala 3 extension methods syntax") {
        extension (s: Status)
          def isActive: Boolean = s == Status.Active
        
        val status = Status.Active
        assertTrue(status.isActive)
      }
    )
