package zio.dynamodb

import zio.schema.DeriveSchema
import zio.test.Assertion.equalTo
import zio.test._

object ReifiedOpticsSpec extends DefaultRunnableSpec {
  final case class Person(name: String, age: Int)

  object Person {
    implicit val schema = DeriveSchema.gen[Person]
    val (name, age)     = ProjectionExpression.accessors[Person]
  }

  override def spec: ZSpec[Environment, Failure] =
    suite("reified optics suite")(
      test("name Lens results is a valid projection expression") {
        val pe: ProjectionExpression = Person.name
        assert(pe)(equalTo(ProjectionExpression.MapElement(ProjectionExpression.Root, "name")))
      },
      test("age Lens results is a valid projection expression") {
        val pe: ProjectionExpression = Person.age
        assert(pe)(equalTo(ProjectionExpression.MapElement(ProjectionExpression.Root, "age")))
      }
    )
}
