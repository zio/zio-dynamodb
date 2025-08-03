package zio.dynamodb.blocks

import zio.test._
import zio.blocks.schema._

object WrapperTypeSpec extends ZIOSpecDefault {
  final case class Wrapper(value: String) extends AnyVal
  object Wrapper                          extends CompanionOptics[Wrapper] {
    implicit val schema: Schema[Wrapper] = Schema.derived
  }
  final case class Foo(w: Wrapper)

  val spec = suite("WrapperTypeSpec")(
  )

}
