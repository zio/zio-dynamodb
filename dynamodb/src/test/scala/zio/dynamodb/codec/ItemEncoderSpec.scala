package zio.dynamodb.codec

import zio.dynamodb.Item
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, ZSpec, _ }

import java.time.Instant

object ItemEncoderSpec extends DefaultRunnableSpec with CodecTestFixtures {
  override def spec: ZSpec[Environment, Failure] =
    suite("ItemEncoder Suite")(mainSuite @@ TestAspect.ignore, isolationSuite)

  val mainSuite: ZSpec[Environment, Failure]      = suite("Main Suite")(
    test("encodes List") {
      val expectedItem: Item = Item("nums" -> List(1, 2, 3))

      val item = ItemEncoder.toItem(CaseClassOfList(List(1, 2, 3)))

      assert(item)(equalTo(expectedItem))
    },
    test("encodes simple Optional Item") {
      val expectedItem: Item = Item("id" -> 2, "name" -> "Avi", "opt" -> null)

      val item = ItemEncoder.toItem(SimpleCaseClass3Option(2, "Avi", opt = None))

      assert(item)(equalTo(expectedItem))
    },
    test("encodes nested Optional Item") {
      val expectedItem: Item = Item("opt" -> 1)

      val item = ItemEncoder.toItem(CaseClassOfNestedOption(opt = Some(Some(1))))

      assert(item)(equalTo(expectedItem))
    },
    test("encodes simple Item") {
      val expectedItem: Item = Item("id" -> 2, "name" -> "Avi", "flag" -> true)

      val item = ItemEncoder.toItem(SimpleCaseClass3(2, "Avi", flag = true))

      assert(item)(equalTo(expectedItem))
    },
    test("encodes nested Items") {
      val expectedItem: Item = Item("id" -> 1, "nested" -> Item("id" -> 2, "name" -> "Avi", "flag" -> true))

      val item = ItemEncoder.toItem(NestedCaseClass2(1, SimpleCaseClass3(2, "Avi", flag = true)))

      assert(item)(equalTo(expectedItem))
    },
    test("encodes LocalDateTime") {
      val expectedItem: Item = Item("instant" -> "2021-09-28T00:00:00Z")

      val item =
        ItemEncoder.toItem(CaseClassOfInstant(Instant.parse("2021-09-28T00:00:00Z")))

      assert(item)(equalTo(expectedItem))
    }
  )
  val isolationSuite: ZSpec[Environment, Failure] = suite("Isolation Suite")(
    test("encodes Ok ADT") {
      val expectedItem: Item = Item("status" -> Item("Ok" -> Item("response" -> List("1", "2"))))

      val item = ItemEncoder.toItem(CaseClassOfStatus(Ok(List("1", "2"))))

      assert(item)(equalTo(expectedItem))
    },
    // case3 = {EnumSchemas$Case@959} "Case(Pending,Transform(Primitive(unit)))"
    test("encodes Pending case object ADT") {
      val expectedItem: Item = Item("status" -> Item("Pending" -> null))

      val item = ItemEncoder.toItem(CaseClassOfStatus(Pending))

      assert(item)(equalTo(expectedItem))
    }
  )
}
