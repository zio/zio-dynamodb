package zio.dynamodb

import zio.dynamodb.Decoder.decoder
import zio.schema.Schema

trait GeneratedCaseClassDecoders {
  def caseClass1Decoder[A1, Z](schema: Schema.CaseClass1[A1, Z]): Decoder[Z] = { (av: AttributeValue) =>
    decodeFields(av, schema.field).map { xs =>
      schema.construct(xs(0).asInstanceOf[A1])
    }
  }

  def caseClass2Decoder[A1, A2, Z](schema: Schema.CaseClass2[A1, A2, Z]): Decoder[Z] = { (av: AttributeValue) =>
    decodeFields(av, schema.field1, schema.field2).map { xs =>
      schema.construct(xs(0).asInstanceOf[A1], xs(1).asInstanceOf[A2])
    }
  }

  def caseClass3Decoder[A1, A2, A3, Z](schema: Schema.CaseClass3[A1, A2, A3, Z]): Decoder[Z] = { (av: AttributeValue) =>
    decodeFields(av, schema.field1, schema.field2, schema.field3).map { xs =>
      schema.construct(xs(0).asInstanceOf[A1], xs(1).asInstanceOf[A2], xs(2).asInstanceOf[A3])
    }
  }

  private def decodeFields(av: AttributeValue, fields: Schema.Field[_]*): Either[String, List[Any]] =
    av match {
      case AttributeValue.Map(map) =>
        zio.dynamodb
          .foreach(fields) {
            case Schema.Field(key, schema, _) =>
              val dec        = decoder(schema)
              val maybeValue = map.get(AttributeValue.String(key))
              maybeValue.map(dec).toRight(s"field '$key' not found in $av").flatten
          }
          .map(_.toList)
      case _                       =>
        Left(s"$av is not an AttributeValue.Map")
    }
}
