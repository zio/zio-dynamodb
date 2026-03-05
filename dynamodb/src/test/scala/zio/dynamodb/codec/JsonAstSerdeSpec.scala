package zio.dynamodb.codec

import zio.Scope
import zio.dynamodb.{ AttributeValue, Codec }
import zio.json.ast.Json
import zio.schema.codec.json.schemaJson
import zio.test.Assertion._
import zio.test.{ assert, Spec, TestEnvironment, ZIOSpecDefault }

object JsonAstSerdeSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("JsonAstSerdeSpec")(
      encoderSuite,
      decoderSuite,
      roundTripSuite
    )

  val encoderSuite = suite("JSON AST to AttributeValue encoder suite")(
    test("encode Json.Str") {
      val json    = Json.Str("hello")
      val encoded = Codec.encoder(schemaJson)(json)

      assert(encoded)(equalTo(AttributeValue.String("hello")))
    },
    test("encode Json.Num") {
      val json    = Json.Num(42.5)
      val encoded = Codec.encoder(schemaJson)(json)

      assert(encoded)(equalTo(AttributeValue.Number(BigDecimal(42.5))))
    },
    test("encode Json.Bool") {
      val json    = Json.Bool(true)
      val encoded = Codec.encoder(schemaJson)(json)

      assert(encoded)(equalTo(AttributeValue.Bool(true)))
    },
    test("encode Json.Null") {
      val json    = Json.Null
      val encoded = Codec.encoder(schemaJson)(json)

      assert(encoded)(equalTo(AttributeValue.Null))
    },
    test("encode Json.Obj") {
      val json    = Json.Obj(
        "name"     -> Json.Str("John"),
        "age"      -> Json.Num(30),
        "active"   -> Json.Bool(true),
        "metadata" -> Json.Null
      )
      val encoded = Codec.encoder(schemaJson)(json)

      val expected     = AttributeValue.Map.empty +
        ("name"     -> AttributeValue.String("John")) +
        ("age"      -> AttributeValue.Number(BigDecimal(30))) +
        ("active"   -> AttributeValue.Bool(true)) +
        ("metadata" -> AttributeValue.Null)

      assert(encoded)(equalTo(expected))
    },
    test("encode Json.Arr") {
      val json    = Json.Arr(
        Json.Str("first"),
        Json.Num(42),
        Json.Bool(false),
        Json.Null
      )
      val encoded = Codec.encoder(schemaJson)(json)

      val expected = AttributeValue.List(
        List(
          AttributeValue.String("first"),
          AttributeValue.Number(BigDecimal(42)),
          AttributeValue.Bool(false),
          AttributeValue.Null
        )
      )

      assert(encoded)(equalTo(expected))
    },
    test("encode nested Json.Obj") {
      val json    = Json.Obj(
        "user"  -> Json.Obj(
          "name" -> Json.Str("Alice"),
          "age"  -> Json.Num(25)
        ),
        "items" -> Json.Arr(
          Json.Str("item1"),
          Json.Str("item2")
        )
      )
      val encoded = Codec.encoder(schemaJson)(json)

      val expected  = AttributeValue.Map.empty +
        ("user"  -> (AttributeValue.Map.empty +
          ("name" -> AttributeValue.String("Alice")) +
          ("age"  -> AttributeValue.Number(BigDecimal(25))))) +
        ("items" -> AttributeValue.List(
          List(
            AttributeValue.String("item1"),
            AttributeValue.String("item2")
          )
        ))

      assert(encoded)(equalTo(expected))
    },
    test("encode empty Json.Obj") {
      val json    = Json.Obj()
      val encoded = Codec.encoder(schemaJson)(json)

      assert(encoded)(equalTo(AttributeValue.Map.empty))
    },
    test("encode empty Json.Arr") {
      val json    = Json.Arr()
      val encoded = Codec.encoder(schemaJson)(json)

      assert(encoded)(equalTo(AttributeValue.List(List.empty)))
    }
  )

  val decoderSuite = suite("AttributeValue to JSON AST decoder suite")(
    test("decode AttributeValue.String to Json") {
      val av      = AttributeValue.String("hello")
      val decoded = Codec.decoder(schemaJson)(av)

      assert(decoded)(isRight(equalTo(Json.Str("hello"))))
    },
    test("decode AttributeValue.Number to Json") {
      val av      = AttributeValue.Number(BigDecimal(42))
      val decoded = Codec.decoder(schemaJson)(av)

      assert(decoded)(isRight(equalTo(Json.Num(42))))
    },
    test("decode AttributeValue.Bool to Json") {
      val av      = AttributeValue.Bool(true)
      val decoded = Codec.decoder(schemaJson)(av)

      assert(decoded)(isRight(equalTo(Json.Bool(true))))
    },
    test("decode AttributeValue.Null to Json") {
      val av      = AttributeValue.Null
      val decoded = Codec.decoder(schemaJson)(av)

      assert(decoded)(isRight(equalTo(Json.Null)))
    },
    test("decode AttributeValue.Map to Json") {
      val av      = AttributeValue.Map.empty +
        ("name"   -> AttributeValue.String("John")) +
        ("age"    -> AttributeValue.Number(BigDecimal(30))) +
        ("active" -> AttributeValue.Bool(true))
      val decoded = Codec.decoder(schemaJson)(av)

      val expected = Json.Obj(
        "name"   -> Json.Str("John"),
        "age"    -> Json.Num(30),
        "active" -> Json.Bool(true)
      )
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("decode AttributeValue.List to Json") {
      val av      = AttributeValue.List(
        List(
          AttributeValue.String("first"),
          AttributeValue.String("second"),
          AttributeValue.Number(BigDecimal(42))
        )
      )
      val decoded = Codec.decoder(schemaJson)(av)

      val expected = Json.Arr(
        Json.Str("first"),
        Json.Str("second"),
        Json.Num(42)
      )
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("decode nested AttributeValue.Map to Json") {
      val av  = AttributeValue.Map.empty +
        ("user"  -> (AttributeValue.Map.empty +
          ("name" -> AttributeValue.String("Alice")) +
          ("age"  -> AttributeValue.Number(BigDecimal(25))))) +
        ("items" -> AttributeValue.List(
          List(
            AttributeValue.String("item1"),
            AttributeValue.String("item2")
          )
        ))

      val decoded = Codec.decoder(schemaJson)(av)

      val expected = Json.Obj(
        "user"  -> Json.Obj(
          "name" -> Json.Str("Alice"),
          "age"  -> Json.Num(25)
        ),
        "items" -> Json.Arr(
          Json.Str("item1"),
          Json.Str("item2")
        )
      )
      assert(decoded)(isRight(equalTo(expected)))
    },
    test("decode empty AttributeValue.Map to Json") {
      val av      = AttributeValue.Map.empty
      val decoded = Codec.decoder(schemaJson)(av)

      assert(decoded)(isRight(equalTo(Json.Obj())))
    },
    test("decode empty AttributeValue.List to Json") {
      val av      = AttributeValue.List(List.empty)
      val decoded = Codec.decoder(schemaJson)(av)

      assert(decoded)(isRight(equalTo(Json.Arr())))
    }
  )

  val roundTripSuite = suite("JSON AST round-trip tests")(
    test("round-trip Json.Str") {
      val original = Json.Str("test string")
      val encoded  = Codec.encoder(schemaJson)(original)
      val decoded  = Codec.decoder(schemaJson)(encoded)

      assert(decoded)(isRight(equalTo(original)))
    },
    test("round-trip Json.Bool") {
      val original = Json.Bool(false)
      val encoded  = Codec.encoder(schemaJson)(original)
      val decoded  = Codec.decoder(schemaJson)(encoded)

      assert(decoded)(isRight(equalTo(original)))
    },
    test("round-trip Json.Null") {
      val original = Json.Null
      val encoded  = Codec.encoder(schemaJson)(original)
      val decoded  = Codec.decoder(schemaJson)(encoded)

      assert(decoded)(isRight(equalTo(original)))
    },
    test("round-trip simple Json.Obj") {
      val original = Json.Obj(
        "name"   -> Json.Str("Bob"),
        "active" -> Json.Bool(true)
      )
      val encoded  = Codec.encoder(schemaJson)(original)
      val decoded  = Codec.decoder(schemaJson)(encoded)

      assert(decoded)(isRight(equalTo(original)))
    },
    test("round-trip Json.Arr") {
      val original = Json.Arr(
        Json.Str("apple"),
        Json.Str("banana"),
        Json.Bool(true)
      )
      val encoded  = Codec.encoder(schemaJson)(original)
      val decoded  = Codec.decoder(schemaJson)(encoded)

      assert(decoded)(isRight(equalTo(original)))
    },
    test("round-trip complex nested Json") {
      val original = Json.Obj(
        "metadata" -> Json.Obj(
          "version" -> Json.Str("1.0"),
          "stable"  -> Json.Bool(true)
        ),
        "tags"     -> Json.Arr(
          Json.Str("important"),
          Json.Str("verified")
        ),
        "config"   -> Json.Obj(
          "enabled"  -> Json.Bool(true),
          "settings" -> Json.Obj(
            "debug" -> Json.Bool(false)
          )
        )
      )
      val encoded  = Codec.encoder(schemaJson)(original)
      val decoded  = Codec.decoder(schemaJson)(encoded)

      assert(decoded)(isRight(equalTo(original)))
    },
    test("round-trip with Json.Num") {
      val original = Json.Obj(
        "count" -> Json.Num(42),
        "pi"    -> Json.Num(3.14159)
      )
      val encoded  = Codec.encoder(schemaJson)(original)
      val decoded  = Codec.decoder(schemaJson)(encoded)

      assert(decoded)(isRight(equalTo(original)))
    },
    test("round-trip empty structures") {
      val original = Json.Obj(
        "empty_object" -> Json.Obj(),
        "empty_array"  -> Json.Arr()
      )
      val encoded  = Codec.encoder(schemaJson)(original)
      val decoded  = Codec.decoder(schemaJson)(encoded)

      assert(decoded)(isRight(equalTo(original)))
    },
    test("round-trip Json.Num as primitive") {
      val original = Json.Num(123.456)
      val encoded  = Codec.encoder(schemaJson)(original)
      val decoded  = Codec.decoder(schemaJson)(encoded)

      assert(decoded)(isRight(equalTo(original)))
    },
    test("round-trip mixed Json types in array") {
      val original = Json.Arr(
        Json.Str("text"),
        Json.Num(42),
        Json.Bool(true),
        Json.Null,
        Json.Obj("key" -> Json.Str("value")),
        Json.Arr(Json.Str("nested"))
      )
      val encoded  = Codec.encoder(schemaJson)(original)
      val decoded  = Codec.decoder(schemaJson)(encoded)

      assert(decoded)(isRight(equalTo(original)))
    }
  )
}
