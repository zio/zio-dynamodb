package zio.dynamodb.codec

import zio.dynamodb._
import zio.test.Assertion._
import zio.test._
import zio.schema.{ DynamicValue, StandardType, TypeId }
import zio.Chunk
import scala.collection.immutable.ListMap

object DynamicCodecSpec extends ZIOSpecDefault with CodecTestFixtures {

  override def spec: Spec[Environment, Any] =
    suite("DynamicCodec Suite")(
      dynamicEncoderSuite,
      dynamicDecoderSuite,
      dynamicRoundTripSuite
    )

  private val dynamicEncoderSuite = suite("DynamicEncoder Suite")(
    test("encodes DynamicValue.Primitive with String") {
      val dynamicValue = DynamicValue.Primitive("test", StandardType.StringType)
      val encoded      = Codec.encoder(zio.schema.Schema.dynamicValue)(dynamicValue)

      assert(encoded)(equalTo(AttributeValue.String("test")))
    },
    test("encodes DynamicValue.Primitive with Int") {
      val dynamicValue = DynamicValue.Primitive(42, StandardType.IntType)
      val encoded      = Codec.encoder(zio.schema.Schema.dynamicValue)(dynamicValue)

      assert(encoded)(equalTo(AttributeValue.Number(BigDecimal(42))))
    },
    test("encodes DynamicValue.Primitive with Boolean") {
      val dynamicValue = DynamicValue.Primitive(true, StandardType.BoolType)
      val encoded      = Codec.encoder(zio.schema.Schema.dynamicValue)(dynamicValue)

      assert(encoded)(equalTo(AttributeValue.Bool(true)))
    },
    test("encodes DynamicValue.Primitive with BigDecimal") {
      val bigDecimal   = BigDecimal("123.45").bigDecimal
      val dynamicValue = DynamicValue.Primitive(bigDecimal, StandardType.BigDecimalType)
      val encoded      = Codec.encoder(zio.schema.Schema.dynamicValue)(dynamicValue)

      assert(encoded)(equalTo(AttributeValue.Number(BigDecimal(bigDecimal))))
    },
    test("encodes DynamicValue.Primitive with Binary") {
      val binary       = Chunk.fromArray("test".getBytes)
      val dynamicValue = DynamicValue.Primitive(binary, StandardType.BinaryType)
      val encoded      = Codec.encoder(zio.schema.Schema.dynamicValue)(dynamicValue)

      assert(encoded)(equalTo(AttributeValue.Binary(binary)))
    },
    test("encodes DynamicValue.Record") {
      val record  = DynamicValue.Record(
        TypeId.Structural,
        ListMap(
          "name" -> DynamicValue.Primitive("John", StandardType.StringType),
          "age"  -> DynamicValue.Primitive(30, StandardType.IntType)
        )
      )
      val encoded = Codec.encoder(zio.schema.Schema.dynamicValue)(record)

      val expected = AttributeValue.Map(
        Map(
          AttributeValue.String("name") -> AttributeValue.String("John"),
          AttributeValue.String("age")  -> AttributeValue.Number(BigDecimal(30))
        )
      )
      assert(encoded)(equalTo(expected))
    },
    test("encodes DynamicValue.Sequence") {
      val sequence = DynamicValue.Sequence(
        Chunk(
          DynamicValue.Primitive("first", StandardType.StringType),
          DynamicValue.Primitive("second", StandardType.StringType)
        )
      )
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(sequence)

      val expected = AttributeValue.List(
        List(
          AttributeValue.String("first"),
          AttributeValue.String("second")
        )
      )
      assert(encoded)(equalTo(expected))
    },
    test("encodes DynamicValue.SetValue with String elements as StringSet") {
      val setValue = DynamicValue.SetValue(
        Set(
          DynamicValue.Primitive("first", StandardType.StringType),
          DynamicValue.Primitive("second", StandardType.StringType)
        )
      )
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(setValue)

      assert(encoded)(equalTo(AttributeValue.StringSet(Set("first", "second"))))
    },
    test("encodes DynamicValue.SetValue with Number elements as NumberSet") {
      val setValue = DynamicValue.SetValue(
        Set(
          DynamicValue.Primitive(BigDecimal(1).bigDecimal, StandardType.BigDecimalType),
          DynamicValue.Primitive(BigDecimal(2).bigDecimal, StandardType.BigDecimalType)
        )
      )
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(setValue)

      assert(encoded)(equalTo(AttributeValue.NumberSet(Set(BigDecimal(1), BigDecimal(2)))))
    },
    test("encodes DynamicValue.SetValue with Binary elements as BinarySet") {
      val binary1  = Chunk.fromArray("test1".getBytes)
      val binary2  = Chunk.fromArray("test2".getBytes)
      val setValue = DynamicValue.SetValue(
        Set(
          DynamicValue.Primitive(binary1, StandardType.BinaryType),
          DynamicValue.Primitive(binary2, StandardType.BinaryType)
        )
      )
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(setValue)

      assert(encoded)(equalTo(AttributeValue.BinarySet(Set(binary1, binary2))))
    },
    test("encodes DynamicValue.SetValue with mixed elements as List") {
      val setValue = DynamicValue.SetValue(
        Set(
          DynamicValue.Primitive("string", StandardType.StringType),
          DynamicValue.Primitive(42, StandardType.IntType)
        )
      )
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(setValue)

      assert(encoded)(isSubtype[AttributeValue.List](anything))
    },
    test("encodes DynamicValue.SomeValue") {
      val someValue = DynamicValue.SomeValue(DynamicValue.Primitive("test", StandardType.StringType))
      val encoded   = Codec.encoder(zio.schema.Schema.dynamicValue)(someValue)

      assert(encoded)(equalTo(AttributeValue.String("test")))
    },
    test("encodes DynamicValue.NoneValue") {
      val noneValue = DynamicValue.NoneValue
      val encoded   = Codec.encoder(zio.schema.Schema.dynamicValue)(noneValue)

      assert(encoded)(equalTo(AttributeValue.Null))
    },
    test("encodes DynamicValue.Tuple") {
      val tuple   = DynamicValue.Tuple(
        DynamicValue.Primitive("left", StandardType.StringType),
        DynamicValue.Primitive(42, StandardType.IntType)
      )
      val encoded = Codec.encoder(zio.schema.Schema.dynamicValue)(tuple)

      val expected = AttributeValue.List(
        List(
          AttributeValue.String("left"),
          AttributeValue.Number(BigDecimal(42))
        )
      )
      assert(encoded)(equalTo(expected))
    },
    test("encodes DynamicValue.LeftValue") {
      val leftValue = DynamicValue.LeftValue(DynamicValue.Primitive("error", StandardType.StringType))
      val encoded   = Codec.encoder(zio.schema.Schema.dynamicValue)(leftValue)

      val expected = AttributeValue.Map(
        Map(
          AttributeValue.String("Left") -> AttributeValue.String("error")
        )
      )
      assert(encoded)(equalTo(expected))
    },
    test("encodes DynamicValue.RightValue") {
      val rightValue = DynamicValue.RightValue(DynamicValue.Primitive(42, StandardType.IntType))
      val encoded    = Codec.encoder(zio.schema.Schema.dynamicValue)(rightValue)

      val expected = AttributeValue.Map(
        Map(
          AttributeValue.String("Right") -> AttributeValue.Number(BigDecimal(42))
        )
      )
      assert(encoded)(equalTo(expected))
    },
    test("encodes DynamicValue.Singleton") {
      val singleton = DynamicValue.Singleton(TypeId.Structural)
      val encoded   = Codec.encoder(zio.schema.Schema.dynamicValue)(singleton)

      assert(encoded)(equalTo(AttributeValue.Map(ListMap.empty)))
    },
    test("throws exception for DynamicValue.Enumeration") {
      val enumValue =
        DynamicValue.Enumeration(TypeId.Structural, "test" -> DynamicValue.Primitive("value", StandardType.StringType))

      assert(
        try {
          Codec.encoder(zio.schema.Schema.dynamicValue)(enumValue)
          false
        } catch {
          case ex: Exception => ex.getMessage.contains("DynamicValue.Enumeration is not supported")
        }
      )(isTrue)
    },
    test("throws exception for DynamicValue.Dictionary") {
      val dictValue = DynamicValue.Dictionary(Chunk.empty)

      assert(
        try {
          Codec.encoder(zio.schema.Schema.dynamicValue)(dictValue)
          false
        } catch {
          case ex: Exception => ex.getMessage.contains("DynamicValue.Dictionary is not supported")
        }
      )(isTrue)
    },
    test("throws exception for DynamicValue.BothValue") {
      val bothValue = DynamicValue.BothValue(
        DynamicValue.Primitive("left", StandardType.StringType),
        DynamicValue.Primitive("right", StandardType.StringType)
      )

      assert(
        try {
          Codec.encoder(zio.schema.Schema.dynamicValue)(bothValue)
          false
        } catch {
          case ex: Exception => ex.getMessage.contains("DynamicValue.BothValue is not supported")
        }
      )(isTrue)
    },
    test("throws exception for DynamicValue.Error") {
      val errorValue = DynamicValue.Error("test error")

      assert(
        try {
          Codec.encoder(zio.schema.Schema.dynamicValue)(errorValue)
          false
        } catch {
          case ex: Exception => ex.getMessage.contains("DynamicValue.Error is not supported")
        }
      )(isTrue)
    }
  )

  private val dynamicDecoderSuite = suite("DynamicDecoder Suite")(
    test("decodes AttributeValue.String to DynamicValue.Primitive") {
      val av      = AttributeValue.String("test")
      val decoded = Codec.decoder(zio.schema.Schema.dynamicValue)(av)

      val expected = DynamicValue.Primitive("test", StandardType.StringType)
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("decodes AttributeValue.Number to DynamicValue.Primitive") {
      val av      = AttributeValue.Number(BigDecimal(42))
      val decoded = Codec.decoder(zio.schema.Schema.dynamicValue)(av)

      val expected = DynamicValue.Primitive(BigDecimal(42).bigDecimal, StandardType.BigDecimalType)
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("decodes AttributeValue.Bool to DynamicValue.Primitive") {
      val av      = AttributeValue.Bool(true)
      val decoded = Codec.decoder(zio.schema.Schema.dynamicValue)(av)

      val expected = DynamicValue.Primitive(true, StandardType.BoolType)
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("decodes AttributeValue.Binary to DynamicValue.Primitive") {
      val binary  = Chunk.fromArray("test".getBytes)
      val av      = AttributeValue.Binary(binary)
      val decoded = Codec.decoder(zio.schema.Schema.dynamicValue)(av)

      val expected = DynamicValue.Primitive(binary, StandardType.BinaryType)
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("decodes AttributeValue.Null to DynamicValue.NoneValue") {
      val av      = AttributeValue.Null
      val decoded = Codec.decoder(zio.schema.Schema.dynamicValue)(av)

      assert(decoded)(isRight(equalTo(DynamicValue.NoneValue)))
    },
    test("decodes AttributeValue.StringSet to DynamicValue.SetValue") {
      val av      = AttributeValue.StringSet(Set("first", "second"))
      val decoded = Codec.decoder(zio.schema.Schema.dynamicValue)(av)

      val expected = DynamicValue.SetValue(
        Set(
          DynamicValue.Primitive("first", StandardType.StringType),
          DynamicValue.Primitive("second", StandardType.StringType)
        )
      )
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("decodes AttributeValue.NumberSet to DynamicValue.SetValue") {
      val av      = AttributeValue.NumberSet(Set(BigDecimal(1), BigDecimal(2)))
      val decoded = Codec.decoder(zio.schema.Schema.dynamicValue)(av)

      val expected = DynamicValue.SetValue(
        Set(
          DynamicValue.Primitive(BigDecimal(1).bigDecimal, StandardType.BigDecimalType),
          DynamicValue.Primitive(BigDecimal(2).bigDecimal, StandardType.BigDecimalType)
        )
      )
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("decodes AttributeValue.BinarySet to DynamicValue.SetValue") {
      val binary1 = Chunk.fromArray("test1".getBytes)
      val binary2 = Chunk.fromArray("test2".getBytes)
      val av      = AttributeValue.BinarySet(Set(binary1, binary2))
      val decoded = Codec.decoder(zio.schema.Schema.dynamicValue)(av)

      val expected = DynamicValue.SetValue(
        Set(
          DynamicValue.Primitive(binary1, StandardType.BinaryType),
          DynamicValue.Primitive(binary2, StandardType.BinaryType)
        )
      )
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("decodes AttributeValue.List to DynamicValue.Sequence") {
      val av      = AttributeValue.List(
        List(
          AttributeValue.String("first"),
          AttributeValue.String("second")
        )
      )
      val decoded = Codec.decoder(zio.schema.Schema.dynamicValue)(av)

      val expected = DynamicValue.Sequence(
        Chunk(
          DynamicValue.Primitive("first", StandardType.StringType),
          DynamicValue.Primitive("second", StandardType.StringType)
        )
      )
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("decodes AttributeValue.Map to DynamicValue.Record") {
      val av      = AttributeValue.Map(
        Map(
          AttributeValue.String("name") -> AttributeValue.String("John"),
          AttributeValue.String("age")  -> AttributeValue.Number(BigDecimal(30))
        )
      )
      val decoded = Codec.decoder(zio.schema.Schema.dynamicValue)(av)

      val expected = DynamicValue.Record(
        TypeId.parse("AttributeValue.Map"),
        ListMap(
          "name" -> DynamicValue.Primitive("John", StandardType.StringType),
          "age"  -> DynamicValue.Primitive(BigDecimal(30).bigDecimal, StandardType.BigDecimalType)
        )
      )
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("decodes empty AttributeValue.List to empty DynamicValue.Sequence") {
      val av      = AttributeValue.List(List.empty)
      val decoded = Codec.decoder(zio.schema.Schema.dynamicValue)(av)

      val expected = DynamicValue.Sequence(Chunk.empty)
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("decodes empty AttributeValue.Map to empty DynamicValue.Record") {
      val av      = AttributeValue.Map(Map.empty)
      val decoded = Codec.decoder(zio.schema.Schema.dynamicValue)(av)

      val expected = DynamicValue.Record(
        TypeId.parse("AttributeValue.Map"),
        ListMap.empty
      )
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("decodes nested AttributeValue.Map") {
      val av      = AttributeValue.Map(
        Map(
          AttributeValue.String("outer") -> AttributeValue.Map(
            Map(
              AttributeValue.String("inner") -> AttributeValue.String("value")
            )
          )
        )
      )
      val decoded = Codec.decoder(zio.schema.Schema.dynamicValue)(av)

      val expected = DynamicValue.Record(
        TypeId.parse("AttributeValue.Map"),
        ListMap(
          "outer" -> DynamicValue.Record(
            TypeId.parse("AttributeValue.Map"),
            ListMap(
              "inner" -> DynamicValue.Primitive("value", StandardType.StringType)
            )
          )
        )
      )
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("decodes nested AttributeValue.List") {
      val av      = AttributeValue.List(
        List(
          AttributeValue.List(
            List(
              AttributeValue.String("nested")
            )
          )
        )
      )
      val decoded = Codec.decoder(zio.schema.Schema.dynamicValue)(av)

      val expected = DynamicValue.Sequence(
        Chunk(
          DynamicValue.Sequence(
            Chunk(
              DynamicValue.Primitive("nested", StandardType.StringType)
            )
          )
        )
      )
      assert(decoded)(isRight(equalTo(expected)))
    }
  )

  private val dynamicRoundTripSuite = suite("Dynamic Round-trip Suite")(
    test("round-trip String primitive") {
      val original = DynamicValue.Primitive("test", StandardType.StringType)
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(original)
      val decoded  = Codec.decoder(zio.schema.Schema.dynamicValue)(encoded)

      assert(decoded)(isRight(equalTo(original)))
    },
    test("round-trip Boolean primitive") {
      val original = DynamicValue.Primitive(true, StandardType.BoolType)
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(original)
      val decoded  = Codec.decoder(zio.schema.Schema.dynamicValue)(encoded)

      assert(decoded)(isRight(equalTo(original)))
    },
    test("round-trip BigDecimal primitive") {
      val original = DynamicValue.Primitive(BigDecimal("123.45").bigDecimal, StandardType.BigDecimalType)
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(original)
      val decoded  = Codec.decoder(zio.schema.Schema.dynamicValue)(encoded)

      assert(decoded.map(_.asInstanceOf[DynamicValue.Primitive[_]].value.asInstanceOf[java.math.BigDecimal]))(
        isRight(equalTo(BigDecimal("123.45").bigDecimal))
      )
    },
    test("round-trip Binary primitive") {
      val binary   = Chunk.fromArray("test".getBytes)
      val original = DynamicValue.Primitive(binary, StandardType.BinaryType)
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(original)
      val decoded  = Codec.decoder(zio.schema.Schema.dynamicValue)(encoded)

      assert(decoded)(isRight(equalTo(DynamicValue.Primitive(binary, StandardType.BinaryType))))
    },
    test("round-trip Record") {
      val original = DynamicValue.Record(
        TypeId.Structural,
        ListMap(
          "name" -> DynamicValue.Primitive("John", StandardType.StringType),
          "age"  -> DynamicValue.Primitive(BigDecimal(30).bigDecimal, StandardType.BigDecimalType)
        )
      )
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(original)
      val decoded  = Codec.decoder(zio.schema.Schema.dynamicValue)(encoded)

      val expected = DynamicValue.Record(
        TypeId.parse("AttributeValue.Map"),
        ListMap(
          "name" -> DynamicValue.Primitive("John", StandardType.StringType),
          "age"  -> DynamicValue.Primitive(BigDecimal(30).bigDecimal, StandardType.BigDecimalType)
        )
      )
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("round-trip Sequence") {
      val original = DynamicValue.Sequence(
        Chunk(
          DynamicValue.Primitive("first", StandardType.StringType),
          DynamicValue.Primitive("second", StandardType.StringType)
        )
      )
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(original)
      val decoded  = Codec.decoder(zio.schema.Schema.dynamicValue)(encoded)

      assert(decoded)(isRight(equalTo(original)))
    },
    test("round-trip StringSet") {
      val original = DynamicValue.SetValue(
        Set(
          DynamicValue.Primitive("first", StandardType.StringType),
          DynamicValue.Primitive("second", StandardType.StringType)
        )
      )
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(original)
      val decoded  = Codec.decoder(zio.schema.Schema.dynamicValue)(encoded)

      assert(decoded)(isRight(equalTo(original)))
    },
    test("round-trip NumberSet") {
      val original = DynamicValue.SetValue(
        Set(
          DynamicValue.Primitive(BigDecimal(1).bigDecimal, StandardType.BigDecimalType),
          DynamicValue.Primitive(BigDecimal(2).bigDecimal, StandardType.BigDecimalType)
        )
      )
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(original)
      val decoded  = Codec.decoder(zio.schema.Schema.dynamicValue)(encoded)

      val expected = DynamicValue.SetValue(
        Set(
          DynamicValue.Primitive(BigDecimal(1).bigDecimal, StandardType.BigDecimalType),
          DynamicValue.Primitive(BigDecimal(2).bigDecimal, StandardType.BigDecimalType)
        )
      )
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("round-trip NoneValue") {
      val original = DynamicValue.NoneValue
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(original)
      val decoded  = Codec.decoder(zio.schema.Schema.dynamicValue)(encoded)

      assert(decoded)(isRight(equalTo(original)))
    },
    test("round-trip SomeValue") {
      val original = DynamicValue.SomeValue(DynamicValue.Primitive("test", StandardType.StringType))
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(original)
      val decoded  = Codec.decoder(zio.schema.Schema.dynamicValue)(encoded)

      // SomeValue encoding unwraps the value, so decoder won't restore the SomeValue wrapper
      val expected = DynamicValue.Primitive("test", StandardType.StringType)
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("round-trip Tuple") {
      val original = DynamicValue.Tuple(
        DynamicValue.Primitive("left", StandardType.StringType),
        DynamicValue.Primitive(BigDecimal(42).bigDecimal, StandardType.BigDecimalType)
      )
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(original)
      val decoded  = Codec.decoder(zio.schema.Schema.dynamicValue)(encoded)

      // Tuple becomes a Sequence after round-trip
      val expected = DynamicValue.Sequence(
        Chunk(
          DynamicValue.Primitive("left", StandardType.StringType),
          DynamicValue.Primitive(BigDecimal(42).bigDecimal, StandardType.BigDecimalType)
        )
      )
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("round-trip LeftValue") {
      val original = DynamicValue.LeftValue(DynamicValue.Primitive("error", StandardType.StringType))
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(original)
      val decoded  = Codec.decoder(zio.schema.Schema.dynamicValue)(encoded)

      // LeftValue becomes a Record after round-trip
      val expected = DynamicValue.Record(
        TypeId.parse("AttributeValue.Map"),
        ListMap(
          "Left" -> DynamicValue.Primitive("error", StandardType.StringType)
        )
      )
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("round-trip RightValue") {
      val original =
        DynamicValue.RightValue(DynamicValue.Primitive(BigDecimal(42).bigDecimal, StandardType.BigDecimalType))
      val encoded  = Codec.encoder(zio.schema.Schema.dynamicValue)(original)
      val decoded  = Codec.decoder(zio.schema.Schema.dynamicValue)(encoded)

      // RightValue becomes a Record after round-trip
      val expected = DynamicValue.Record(
        TypeId.parse("AttributeValue.Map"),
        ListMap(
          "Right" -> DynamicValue.Primitive(BigDecimal(42).bigDecimal, StandardType.BigDecimalType)
        )
      )
      assert(decoded)(isRight(equalTo(expected)))
    }
  )
}
