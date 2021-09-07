package zio.dynamodb.examples.codec

import zio.Chunk
import zio.schema.Schema

object Models {
  final case class NestedCaseClass2(id: Int, nested: SimpleCaseClass3)
  final case class SimpleCaseClass3(id: Int, name: String, flag: Boolean)

  lazy implicit val simpleCaseClass3Schema = Schema.CaseClass3[Int, String, Boolean, SimpleCaseClass3](
    Chunk.empty,
    Schema.Field("id", Schema[Int]),
    Schema.Field("name", Schema[String]),
    Schema.Field("flag", Schema[Boolean]),
    SimpleCaseClass3,
    _.id,
    _.name,
    _.flag
  )

  val simpleCaseClass2Schema = Schema.CaseClass2[Int, SimpleCaseClass3, NestedCaseClass2](
    Chunk.empty,
    Schema.Field("id", Schema[Int]),
    Schema.Field("nested", Schema[SimpleCaseClass3]),
    NestedCaseClass2,
    _.id,
    _.nested
  )

}
