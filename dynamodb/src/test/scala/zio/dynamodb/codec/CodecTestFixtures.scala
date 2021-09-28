package zio.dynamodb.codec

import zio.schema.{ DeriveSchema, Schema, StandardType }

import java.time.format.DateTimeFormatter

trait CodecTestFixtures {

  lazy implicit val nestedCaseClass2Schema: Schema[NestedCaseClass2]               = DeriveSchema.gen[NestedCaseClass2]
  lazy implicit val simpleCaseClass3Schema: Schema[SimpleCaseClass3]               = DeriveSchema.gen[SimpleCaseClass3]
  lazy implicit val simpleCaseClass3SchemaOptional: Schema[SimpleCaseClass3Option] =
    DeriveSchema.gen[SimpleCaseClass3Option]
  lazy implicit val caseClassOfList: Schema[CaseClassOfList]                       = DeriveSchema.gen[CaseClassOfList]
  lazy implicit val caseClassOfOption: Schema[CaseClassOfOption]                   = DeriveSchema.gen[CaseClassOfOption]
  lazy implicit val caseClassOfNestedOption: Schema[CaseClassOfNestedOption]       = DeriveSchema.gen[CaseClassOfNestedOption]
  lazy implicit val caseClassOfEither: Schema[CaseClassOfEither]                   = DeriveSchema.gen[CaseClassOfEither]
  lazy implicit val caseClassOfTuple3: Schema[CaseClassOfTuple3]                   = DeriveSchema.gen[CaseClassOfTuple3]
  lazy implicit val instantSchema                                                  =
    Schema.Primitive(StandardType.Instant(DateTimeFormatter.ISO_INSTANT))
  lazy implicit val caseClassOfInstant: Schema[CaseClassOfInstant]                 =
    DeriveSchema.gen[CaseClassOfInstant]

}
