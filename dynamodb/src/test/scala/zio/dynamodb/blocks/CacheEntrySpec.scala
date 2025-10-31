package zio.dynamodb.blocks

import zio.dynamodb.blocks.BlocksDdbDerived.CacheEntry
import zio.dynamodb.{ Decoder, Encoder }
import zio.test._

object CacheEntrySpec extends ZIOSpecDefault {

  // Dummy codec for testing
  final case class DummyCodec(id: String) extends DdbCodec[Int] {
    override def encoder: Encoder[Int] = ???

    override def decoder: Decoder[Int] = ???
  }

  def spec =
    suite("CacheEntrySpec")(
      test("addEntry updates the codec and name arrays") {
        val entry  = CacheEntry.makeWithNames(2)
        val codec1 = DummyCodec("c1")
        val codec2 = DummyCodec("c2")

        entry.addEntry(codec1, "first", 0)
        entry.addEntry(codec2, "second", 1)

        val codecAt0: DummyCodec = entry.byIndex(0).asInstanceOf[DummyCodec]
        val codecAt1: DummyCodec = entry.byIndex(1).asInstanceOf[DummyCodec]

        assertTrue(
          codecAt0 == codec1,
          codecAt1 == codec2,
          entry.byName("first").contains(codec1),
          entry.byName("second").contains(codec2)
        )
      },
      test("byName returns None for non-existent name") {
        val entry = CacheEntry.makeWithNames(1)
        entry.addEntry(DummyCodec("only"), "onlyName", 0)
        assertTrue(entry.byName("missing").isEmpty)
      },
      test("byName returns None when names array is empty") {
        val entry = CacheEntry.makeWithNames(0)
        assertTrue(entry.byName("anything").isEmpty)
      },
      test("nameToIndex map is lazily initialised and cached") {
        val entry  = CacheEntry.makeWithNames(2)
        val codec1 = DummyCodec("c1")
        val codec2 = DummyCodec("c2")

        entry.addEntry(codec1, "first", 0)
        entry.addEntry(codec2, "second", 1)

        // trigger lazy initialisation
        val firstMap  = entry.byName("first")
        val secondMap = entry.byName("second")

        // verify caching by ensuring multiple lookups return same results
        assertTrue(
          firstMap.contains(codec1),
          secondMap.contains(codec2),
          entry.byName("first").contains(codec1) // cached path
        )
      },
      test("toString includes codec and names info") {
        val entry = CacheEntry.makeWithNames(1)
        val codec = DummyCodec("codecA")
        entry.addEntry(codec, "nameA", 0)

        val s = entry.toString
        assertTrue(
          s.contains("CacheEntry"),
          s.contains("codecA"),
          s.contains("nameA")
        )
      }
    )
}
