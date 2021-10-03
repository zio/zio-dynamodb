package zio.dynamodb.codec

import zio.random.Random
import zio.schema.Schema
import zio.test.Assertion.{ equalTo, isRight }
import zio.test.{ ZSpec, _ }

object RoundTripSpec extends DefaultRunnableSpec with CodecTestFixtures {

  override def spec: ZSpec[Environment, Failure] = mainSuite

  val mainSuite = suite("main suite")(
    testM("encodes a primitive") {
      checkM(SchemaGen.anyPrimitiveAndGen) {
        case (schema, gen) =>
          assertEncodes(schema, gen)
      }
    }
  )

  private def assertEncodes[A](schema: Schema[A], genA: Gen[Random with Sized, A]) = {
    val enc = ItemEncoder.encoder(schema)
    val dec = ItemDecoder.decoder(schema)

    check(genA) { a =>
      val encValue = enc(a)
      val decValue = dec(enc(a))
      decValue match {
        case Left(x) => println(s"XXXXXXXXXX err=$x a=$a enc=$encValue")
        case _       => ()
      }
      assert(dec(enc(a)))(isRight(equalTo(a)))
    }

  }

}
