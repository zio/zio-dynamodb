//package zio.dynamodb.benchmarks
//
//import java.util.concurrent.TimeUnit
//import org.openjdk.jmh.annotations._
//import zio.dynamodb._
//import zio.blocks.schema._
//import scala.util._
//import zio.dynamodb.blocks.BlocksCodec
//
///*
//TO RUN
//sbt "zio-dynamodb-benchmarks/jmh:run -i 10 -wi 5 -f1 -t1 -prof gc .*EitherEncoderBenchmark.*"
//
//ORIGINAL either coder
//[info] the benchmarking environment is safe on JVM/OS/HW level, ask for reviews from the domain experts.
//[info] Do not assume the numbers tell you what you want them to tell.
//[info] Benchmark                                                 (caseType)   Mode  Cnt     Score     Error   Units
//[info] EitherEncoderBenchmark.originalEncode                           Left  thrpt   10  8381.052 ± 439.247  ops/ms
//[info] EitherEncoderBenchmark.originalEncode:gc.alloc.rate             Left  thrpt   10  3964.364 ± 207.770  MB/sec
//[info] EitherEncoderBenchmark.originalEncode:gc.alloc.rate.norm        Left  thrpt   10   496.000 ±   0.001    B/op
//[info] EitherEncoderBenchmark.originalEncode:gc.count                  Left  thrpt   10   541.000            counts
//[info] EitherEncoderBenchmark.originalEncode:gc.time                   Left  thrpt   10   293.000                ms
//[info] EitherEncoderBenchmark.originalEncode                          Right  thrpt   10  8044.322 ± 547.980  ops/ms
//[info] EitherEncoderBenchmark.originalEncode:gc.alloc.rate            Right  thrpt   10  3805.080 ± 259.205  MB/sec
//[info] EitherEncoderBenchmark.originalEncode:gc.alloc.rate.norm       Right  thrpt   10   496.000 ±   0.001    B/op
//[info] EitherEncoderBenchmark.originalEncode:gc.count                 Right  thrpt   10   520.000            counts
//[info] EitherEncoderBenchmark.originalEncode:gc.time                  Right  thrpt   10   250.000                ms
//[success] Total time: 310 s (0:05:10.0), completed 19 Aug 2025, 09:03:12
//
//REFACTORDED either coder
//[info] the benchmarking environment is safe on JVM/OS/HW level, ask for reviews from the domain experts.
//[info] Do not assume the numbers tell you what you want them to tell.
//[info] Benchmark                                                 (caseType)   Mode  Cnt      Score      Error   Units
//[info] EitherEncoderBenchmark.originalEncode                           Left  thrpt   10   7607.988 ± 2904.086  ops/ms
//[info] EitherEncoderBenchmark.originalEncode:gc.alloc.rate             Left  thrpt   10   3714.782 ± 1417.989  MB/sec
//[info] EitherEncoderBenchmark.originalEncode:gc.alloc.rate.norm        Left  thrpt   10    512.000 ±    0.001    B/op
//[info] EitherEncoderBenchmark.originalEncode:gc.count                  Left  thrpt   10    508.000             counts
//[info] EitherEncoderBenchmark.originalEncode:gc.time                   Left  thrpt   10    228.000                 ms
//[info] EitherEncoderBenchmark.originalEncode                          Right  thrpt   10  10154.825 ±   43.274  ops/ms
//[info] EitherEncoderBenchmark.originalEncode:gc.alloc.rate            Right  thrpt   10   4803.387 ±   20.464  MB/sec
//[info] EitherEncoderBenchmark.originalEncode:gc.alloc.rate.norm       Right  thrpt   10    496.000 ±    0.001    B/op
//[info] EitherEncoderBenchmark.originalEncode:gc.count                 Right  thrpt   10    543.000             counts
//[info] EitherEncoderBenchmark.originalEncode:gc.time                  Right  thrpt   10    248.000                 ms
// */
//
//object Data {
//  final case class Person(e: Either[String, String])
//  object Person extends CompanionOptics[Person] {
//    implicit val schema: Schema[Person] = Schema.derived[Person]
//  }
//
//}
//
//// Run mode: ops/sec
//@BenchmarkMode(Array(Mode.Throughput))
//@OutputTimeUnit(TimeUnit.MILLISECONDS)
//@State(Scope.Thread)
//class EitherEncoderBenchmark {
//
//  // Param lets us choose between Left/Right at runtime
//  @Param(Array("Left", "Right"))
//  var caseType: String = _
//
//  var testValue: Data.Person = _
//
//  var encoder: Encoder[Data.Person] = _ => AttributeValue.Null
//
//  @Setup(Level.Iteration)
//  def setup(): Unit = {
//
//    encoder = BlocksCodec.encoder[Data.Person](Data.Person.schema)
//
//    caseType match {
//      case "Left"  => testValue = Data.Person(Left("left-test"))
//      case "Right" => testValue = Data.Person(Right("right-test"))
//    }
//  }
//
//  @Benchmark
//  def encode(): AttributeValue =
//    encoder(testValue)
//
//}
//
//object Tester extends App {
//  val benchmark = new EitherEncoderBenchmark
//  benchmark.setup()
//  println(benchmark.encode())
//}
