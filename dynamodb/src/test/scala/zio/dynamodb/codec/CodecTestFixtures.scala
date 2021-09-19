package zio.dynamodb.codec

import zio.schema.{ DeriveSchema, Schema }

trait CodecTestFixtures {

  lazy implicit val nestedCaseClass2Schema: Schema[NestedCaseClass2] = DeriveSchema.gen[NestedCaseClass2]
  lazy implicit val simpleCaseClass3Schema: Schema[SimpleCaseClass3] = DeriveSchema.gen[SimpleCaseClass3]

}
