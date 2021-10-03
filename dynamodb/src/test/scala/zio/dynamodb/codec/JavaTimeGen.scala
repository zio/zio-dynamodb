package zio.dynamodb.codec

import zio.random.Random
import zio.test.Gen

import java.time.Instant

object JavaTimeGen {
  val anyInstant: Gen[Random, Instant] = Gen
    .long(Instant.MIN.getEpochSecond, Instant.MAX.getEpochSecond)
    .zipWith(Gen.int(Instant.MIN.getNano, Instant.MAX.getNano)) { (seconds, nanos) =>
      Instant.ofEpochSecond(seconds, nanos.toLong)
    }
}
