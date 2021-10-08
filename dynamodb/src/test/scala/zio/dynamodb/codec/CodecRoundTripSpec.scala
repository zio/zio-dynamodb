package zio.dynamodb.codec

import zio.random.Random
import zio.schema.Schema
import zio.test.Assertion.{ equalTo, isRight }
import zio.test.{ ZSpec, _ }

object CodecRoundTripSpec extends DefaultRunnableSpec with CodecTestFixtures {

  override def spec: ZSpec[Environment, Failure] = mainSuite

  val mainSuite = suite("encode and decode round trip suite")(
    testM("a primitive") {
      checkM(SchemaGen.anyPrimitiveAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodes(schema, gen)
      }
    },
    testM("either of primitive") {
      checkM(SchemaGen.anyEitherAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodes(schema, gen)
      }
    }
  )

  private def assertEncodesThenDecodes[A](schema: Schema[A], genA: Gen[Random with Sized, A]) = {
    val enc = ItemEncoder.encoder(schema)
    val dec = ItemDecoder.decoder(schema)

    check(genA) { a =>
      assert(dec(enc(a)))(isRight(equalTo(a)))
    }

  }

}
