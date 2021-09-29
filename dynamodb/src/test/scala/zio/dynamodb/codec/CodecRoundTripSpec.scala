package zio.dynamodb.codec

import zio.schema.codec.JsonCodec

/*
  type Encoder[A] = A => AttributeValue
  type Decoder[+A] = AttributeValue => Either[String, A]
 */

object CodecRoundTripSpec extends App with CodecTestFixtures {
  /*
  QUESTIONS:
  - what is GenericRecord? do we have to provide a direct mapping for it?
   */

  // Either
  // {"aOrb":{"Right":1}} => Item("aOrb" -> Item("Right" -> 1))
  val json = new String(JsonCodec.encode(caseClassOfEither)(CaseClassOfEither(Right(1))).toArray)
  println(json)

  // Option
  // Some(Some(1)) == {"opt":1}
  val json2 = new String(JsonCodec.encode(caseClassOfNestedOption)(CaseClassOfNestedOption(Some(Some(1)))).toArray)
  println(json2)

  // Tuple
  // {"tuple":[[1,2],3]} => Item("tuple" -> List(List(1,2),3))
  val json3 = new String(JsonCodec.encode(caseClassOfTuple3)(CaseClassOfTuple3((1, 2, 3))).toArray)
  println(json3)

  // ADT
  // {"Ok":{"response":["1","2"]}} => Item("Ok" -> Item("response" -> List(1, 2))) ????
//  val json4 = new String(JsonCodec.encode(statusSchema)(Status.Ok(List("1", "2"))).toArray)
//  println(json4)

}
