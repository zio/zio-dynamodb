package zio.dynamodb.codec

import zio.dynamodb.Codec
import zio.schema.{ DeriveSchema, Schema }
import zio.test.{ assertTrue, ZIOSpecDefault }

object CodecExperimentSpec extends ZIOSpecDefault {
  final case class RecordWithTuple(tuple: (List[Int], Int))
  object RecordWithTuple {
    implicit val schema: Schema[RecordWithTuple] = DeriveSchema.gen[RecordWithTuple]
  }

  /*
final case class RecordWithTuple(tuple: (Int, Int, Int))
List(Chunk(List(Chunk(Number(1),Number(2))),Number(3)))))
final case class RecordWithTuple(tuple: (List[Int], Int))
List(Chunk(List(Chunk(Number(1),Number(2))),Number(3)))))

reverse iterate over outer list
create acc list
get first item of l - add to acc
get next element of l
  - if list, recurse
  - else add to acc
get last item of l
reverse acc
   */

  val spec = suite("CodecSpec")(
    test("tuple3") {
      val enc = Codec.encoder[RecordWithTuple](RecordWithTuple.schema)
      val x   = enc(RecordWithTuple((List(1, 2), 3)))
      println(s"XXXXXXXXXXXX encoded $x")
      assertTrue(true)
    }
  )

}
