// package zio.dynamodb.codec

// import zio.Chunk
// import zio.schema._
// import zio.test.Assertion

// object SchemaAssertions {

//   def hasSameSchema[A](expected: Schema[A]): Assertion[Schema[A]] =
//     Assertion.assertion("hasSameSchema")(actual => equalsSchema(expected, actual))

//   def hasSameAst(expected: Schema[_]): Assertion[Schema[_]] =
//     Assertion.assertion("hasSameAst")(actual => equalsAst(expected, actual))

//   private def equalsAst(expected: Schema[_], actual: Schema[_], depth: Int = 0): Boolean =
//     (expected, actual) match {
//       case (Schema.Primitive(StandardType.DurationType, _), Schema.Primitive(StandardType.DurationType, _))     => true
//       case (Schema.Primitive(StandardType.InstantType, _), Schema.Primitive(StandardType.InstantType, _))       => true
//       case (Schema.Primitive(StandardType.LocalDateType, _), Schema.Primitive(StandardType.LocalDateType, _))   =>
//         true
//       case (Schema.Primitive(StandardType.LocalTimeType, _), Schema.Primitive(StandardType.LocalTimeType, _))   =>
//         true
//       case (
//             Schema.Primitive(StandardType.LocalDateTimeType, _),
//             Schema.Primitive(StandardType.LocalDateTimeType, _)
//           ) =>
//         true
//       case (
//             Schema.Primitive(StandardType.ZonedDateTimeType, _),
//             Schema.Primitive(StandardType.ZonedDateTimeType, _)
//           ) =>
//         true
//       case (Schema.Primitive(StandardType.OffsetTimeType, _), Schema.Primitive(StandardType.OffsetTimeType, _)) =>
//         true
//       case (
//             Schema.Primitive(StandardType.OffsetDateTimeType, _),
//             Schema.Primitive(StandardType.OffsetDateTimeType, _)
//           ) =>
//         true
//       case (Schema.Primitive(tpe1, _), Schema.Primitive(tpe2, _))                                               => tpe1 == tpe2
//       case (Schema.Optional(expected, _), Schema.Optional(actual, _))                                           => equalsAst(expected, actual, depth)
//       case (Schema.Tuple2(expectedLeft, expectedRight, _), Schema.Tuple2(actualLeft, actualRight, _))           =>
//         equalsAst(expectedLeft, actualLeft, depth) && equalsAst(expectedRight, actualRight, depth)
//       case (Schema.Tuple2(expectedLeft, expectedRight, _), Schema.GenericRecord(_, structure, _))               =>
//         structure.toChunk.size == 2 &&
//           structure.toChunk.find(_.name == "left").exists(f => equalsAst(expectedLeft, f.schema, depth)) &&
//           structure.toChunk.find(_.name == "right").exists(f => equalsAst(expectedRight, f.schema, depth))
//       case (Schema.Either(expectedLeft, expectedRight, _), Schema.Either(actualLeft, actualRight, _))           =>
//         equalsAst(expectedLeft, actualLeft, depth) && equalsAst(expectedRight, actualRight, depth)
//       case (Schema.Either(expectedLeft, expectedRight, _), right: Schema.Enum[_])                               =>
//         right.cases.size == 2 &&
//           right.caseOf("left").exists(actualLeft => equalsAst(expectedLeft, actualLeft.schema, depth)) &&
//           right.caseOf("right").exists(actualRight => equalsAst(expectedRight, actualRight.schema, depth))

//       // right.structure.size == 2 &&
//       //   right.structure.get("left").exists(actualLeft => equalsAst(expectedLeft, actualLeft, depth)) &&
//       //   right.structure.get("right").exists(actualRight => equalsAst(expectedRight, actualRight, depth))
//       case (Schema.Sequence(expected, _, _, _, _), Schema.Sequence(actual, _, _, _, _))                         =>
//         equalsAst(expected, actual, depth)
//       case (expected: Schema.Record[_], actual: Schema.Record[_])                                               =>
//         expected.fields.zipAll(actual.fields).forall {
//           case (
//                 Some(Schema.Field(expectedLabel, expectedSchema, _, _, _, _)),
//                 Some(Schema.Field(actualLabel, actualSchema, _, _, _, _))
//               ) =>
//             expectedLabel == actualLabel && equalsAst(expectedSchema, actualSchema, depth)
//           case _ => false
//         }
//       // case (expected: Schema.Enum[_], actual: Schema.Enum[_])                                                   =>
//       //   Chunk.fromIterable(expected.fields).zipAll(Chunk.fromIterable(actual.fields)).forall {
//       //     case (Some((expectedId, expectedSchema)), Some((actualId, actualSchema))) =>
//       //       actualId == expectedId && equalsAst(expectedSchema, actualSchema, depth)
//       //     case _                                                                    => false
//       //   }
//       case (expected: Schema.Enum[_], actual: Schema.Enum[_])                                                   =>
//         expected.cases.zipAll(actual.cases).forall {
//           case (Some(expectedCase), Some(actualCase)) =>
//             actualCase.id == expectedCase.id && equalsAst(expectedCase.schema, actualCase.schema, depth)
//           case _                                      => false
//         }
//       case (expected, Schema.Transform(actualSchema, _, _, _, _))                                               =>
//         equalsAst(expected, actualSchema, depth)
//       case (Schema.Transform(expected, _, _, _, _), actual)                                                     =>
//         equalsAst(expected, actual, depth)
//       case (expected: Schema.Lazy[_], actual)                                                                   => if (depth > 10) true else equalsAst(expected.schema, actual, depth + 1)
//       case (expected, actual: Schema.Lazy[_])                                                                   => if (depth > 10) true else equalsAst(expected, actual.schema, depth + 1)
//       case _                                                                                                    => false
//     }

//   private def equalsSchema[A](left: Schema[A], right: Schema[A]): Boolean =
//     (left: Schema[_], right: Schema[_]) match {
//       case (Schema.Transform(codec1, _, _, a1, _), Schema.Transform(codec2, _, _, a2, _))   =>
//         equalsSchema(codec1, codec2) && equalsAnnotations(a1, a2)
//       case (Schema.GenericRecord(structure1, a1), Schema.GenericRecord(structure2, a2))     =>
//         hasSameFields(structure1.toChunk, structure2.toChunk) &&
//           structure1.toChunk.forall {
//             case Schema.Field(label, schema, _) =>
//               val left: Schema[Any]  = schema.asInstanceOf[Schema[Any]]
//               val right: Schema[Any] = structure2.toChunk.find(_.label == label).asInstanceOf[Schema[Any]]
//               equalsSchema(left, right)
//           } && equalsAnnotations(a1, a2)
//       case (left: Schema.Record[_], right: Schema.Record[_])                                =>
//         hasSameStructure(
//           left.asInstanceOf[Schema.Record[A]],
//           right.asInstanceOf[Schema.Record[A]]
//         ) && equalsAnnotations(
//           left.annotations,
//           right.annotations
//         )
//       case (Schema.Sequence(element1, _, _, a1, _), Schema.Sequence(element2, _, _, a2, _)) =>
//         equalsSchema(element1, element2) && equalsAnnotations(a1, a2)
//       case (Schema.Primitive(standardType1, a1), Schema.Primitive(standardType2, a2))       =>
//         standardType1 == standardType2 && equalsAnnotations(a1, a2)
//       case (Schema.Tuple(left1, right1, a1), Schema.Tuple(left2, right2, a2))               =>
//         equalsSchema(left1, left2) && equalsSchema(right1, right2) && equalsAnnotations(a1, a2)
//       case (Schema.Optional(codec1, a1), Schema.Optional(codec2, a2))                       =>
//         equalsSchema(codec1, codec2) && equalsAnnotations(a1, a2)
//       case (Schema.Enum1(l, lA), Schema.Enum1(r, rA))                                       => equalsCase(l, r) && lA.equals(rA)
//       case (Schema.Enum2(l1, l2, lA), Schema.Enum2(r1, r2, rA))                             =>
//         hasSameCases(Seq(l1, l2), Seq(r1, r2)) && equalsAnnotations(lA, rA)
//       case (Schema.Enum3(l1, l2, l3, lA), Schema.Enum3(r1, r2, r3, rA))                     =>
//         hasSameCases(Seq(l1, l2, l3), Seq(r1, r2, r3)) && lA.equals(rA)
//       case (Schema.EnumN(ls, lA), Schema.EnumN(rs, rA))                                     => hasSameCases(ls.toSeq, rs.toSeq) && lA.equals(rA)
//       case (l @ Schema.Lazy(_), r @ Schema.Lazy(_))                                         =>
//         equalsSchema(l.schema.asInstanceOf[Schema[Any]], r.schema.asInstanceOf[Schema[Any]])
//       case (lazySchema @ Schema.Lazy(_), eagerSchema)                                       =>
//         equalsSchema(lazySchema.schema.asInstanceOf[Schema[Any]], eagerSchema.asInstanceOf[Schema[Any]])
//       case (eagerSchema, lazySchema @ Schema.Lazy(_))                                       =>
//         equalsSchema(lazySchema.asInstanceOf[Schema[Any]], eagerSchema.asInstanceOf[Schema[Any]])
//       case _                                                                                => false
//     }

//   private def equalsAnnotations(l: Chunk[Any], r: Chunk[Any]): Boolean = l.equals(r)

//   private def equalsCase(left: Schema.Case[_, _], right: Schema.Case[_, _]): Boolean =
//     left.id == right.id && equalsSchema(left.codec.asInstanceOf[Schema[Any]], right.codec.asInstanceOf[Schema[Any]])

//   private def hasSameCases(ls: Seq[Schema.Case[_, _]], rs: Seq[Schema.Case[_, _]]): Boolean =
//     ls.map(l => rs.exists(r => equalsCase(l, r))).reduce(_ && _) && rs
//       .map(r => ls.exists(l => equalsCase(l, r)))
//       .reduce(_ && _)

//   private def hasSameStructure[A](left: Schema.Record[A], right: Schema.Record[A]): Boolean =
//     left.structure.zip(right.structure).forall {
//       case (Schema.Field(lLabel, lSchema, lAnnotations), Schema.Field(rLabel, rSchema, rAnnotations)) =>
//         lLabel == rLabel && lAnnotations.toSet == rAnnotations.toSet && equalsSchema(lSchema, rSchema)
//     }

//   private def hasSameFields(left: Chunk[Schema.Field[_]], right: Chunk[Schema.Field[_]]): Boolean =
//     left.map(_.label) == right.map(_.label)

// }
