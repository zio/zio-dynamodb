package zio.dynamodb.benchmarks.codecs

import org.openjdk.jmh.annotations.{ Scope => JScope, _ }

import java.util.concurrent.TimeUnit

/**
 * borrows heavily from Andriy Plokhotnyuk's zio-blocks benchmarks https://github.com/zio/zio-blocks
 */
@State(JScope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
abstract class BaseBenchmark
