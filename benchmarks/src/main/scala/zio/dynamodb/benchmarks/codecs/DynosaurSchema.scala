package zio.dynamodb.benchmarks.codecs

import cats.implicits._
import dynosaur.Schema
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
object DynosaurSchema {
  import BenchmarkDomain._

  implicit val schema: Schema[TrafficLight] = Schema.attributeValue.imap[TrafficLight] { av =>
    av.s() match {
      case "Red"    => TrafficLight.Red
      case "Yellow" => TrafficLight.Yellow
      case "Green"  => TrafficLight.Green
      case other    => throw new Exception(s"Unknown TrafficLight: $other")
    }
  } { trafficLight =>
    // Encode TrafficLight to DynamoDB String
    val name = trafficLight match {
      case TrafficLight.Red    => "Red"
      case TrafficLight.Yellow => "Yellow"
      case TrafficLight.Green  => "Green"
    }
    AttributeValue.builder().s(name).build()
  }

  implicit val mapSchema: Schema[Map[String, Int]] = Schema.dict

  implicit val tupleSchema: Schema[(Int, Long, String)] = Schema.record[(Int, Long, String)] { field =>
    (
      field("0", _._1)(Schema.int),
      field("1", _._2)(Schema.long),
      field("2", _._3)(Schema.string)
    ).mapN((a, b, c) => (a, b, c))
  }

  implicit val personSchema: Schema[Person] =
    Schema.record { field =>
      (
        field("id", _.id),
        field("name", _.name),
        field("age", _.age),
        field.opt("address", _.address),
//        field("map", _.map),
//        field("list", _.list),
//        field("tuple", _.tuple),
        field("light", _.light)
      ).mapN(Person.apply)
    }

}
