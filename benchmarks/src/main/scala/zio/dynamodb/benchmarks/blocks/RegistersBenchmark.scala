package zio.dynamodb.benchmarks.blocks

import org.openjdk.jmh.annotations._
import zio.blocks.schema.binding.RegisterOffset.RegisterOffset
import zio.blocks.schema.binding.{ Binding, RegisterOffset, Registers }
import zio.blocks.schema.derive.BindingInstance
import zio.blocks.schema.{ CompanionOptics, Schema }
import zio.dynamodb.benchmarks.blocks.RegistersDomain._
import zio.dynamodb.blocks.{ BlocksDdbDerived2, DdbCodec }

/**
 * borrows heavily from Andriy Plokhotnyuk's zio-blocks benchmarks https://github.com/zio/zio-blocks
 */
class RegistersBenchmark extends BaseBenchmark {
  import RegistersDomain._

  @Param(Array("1", "10", "100", "1000", "10000", "100000"))
  var size: Int                   = 1000
  var listOfRecords: List[Person] = _

  @Setup
  def setup(): Unit =
    listOfRecords = (1 to size)
      .map(_ =>
        Person(
          12345678901L,
          "John",
          30,
          "123 Main St"
        )
      )
      .toList

  @Benchmark
  def encodeUsingRegisters(): Seq[(Long, AnyRef, RegisterOffset, AnyRef)] =
    listOfRecords.map(ExerciseRegistersNoCache.encode)

  @Benchmark
  def encodeUsingCachedRegisters(): Seq[(Long, AnyRef, RegisterOffset, AnyRef)] =
    listOfRecords.map(ExerciseRegistersWithCache.encode)

}
object ExerciseRegistersNoCache {
  def encode(p: Person): (Long, AnyRef, Int, AnyRef) = {
    val reflect       = RegistersDomain.Person.blocksSchema.reflect
    val record        = reflect.asRecord.get
    val recordBinding =
      try record.recordBinding.asInstanceOf[Binding.Record[Person]]
      catch {
        case _: Exception =>
          record.recordBinding
            .asInstanceOf[BindingInstance[DdbCodec, ?, Person]]
            .binding
            .asInstanceOf[Binding.Record[Person]]
      }

    // encode
    val registers     = Registers(record.usedRegisters)
    val deconstructor = recordBinding.deconstructor
    deconstructor.deconstruct(registers, RegisterOffset.Zero, p)
    var offset        = RegisterOffset.Zero
    val id            = registers.getLong(offset, 0)
    offset = RegisterOffset.add(offset, RegisterOffset(longs = 1))
    val name          = registers.getObject(offset, 0)
    offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
    val age           = registers.getInt(offset, 0)
    offset = RegisterOffset.add(offset, RegisterOffset(ints = 1))
    val address       = registers.getObject(offset, 0)
    offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
    (id, name, age, address)
  }

  def decode(p: Person): Unit = {
    val reflect       = RegistersDomain.Person.blocksSchema.reflect
    val record        = reflect.asRecord.get
    val recordBinding =
      try record.recordBinding.asInstanceOf[Binding.Record[Person]]
      catch {
        case _: Exception =>
          record.recordBinding
            .asInstanceOf[BindingInstance[DdbCodec, ?, Person]]
            .binding
            .asInstanceOf[Binding.Record[Person]]
      }

    // encode
    val registers     = Registers(record.usedRegisters)
    val deconstructor = recordBinding.deconstructor
    deconstructor.deconstruct(registers, RegisterOffset.Zero, p)
    var offset        = RegisterOffset.Zero
    registers.getLong(offset, 0)
    offset = RegisterOffset.add(offset, RegisterOffset(longs = 1))
    registers.getObject(offset, 0)
    offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
    registers.getInt(offset, 0)
    offset = RegisterOffset.add(offset, RegisterOffset(ints = 1))
    registers.getObject(offset, 0)
    offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
  }

}

object ExerciseRegistersWithCache {
  final class CachedOffsets(
    val id: RegisterOffset,
    val name: RegisterOffset,
    val age: RegisterOffset,
    val address: RegisterOffset
  )

  @volatile
  var cachedOffsets: CachedOffsets = {
    var offset        = RegisterOffset.Zero
    val idOffset      = offset
    offset = RegisterOffset.add(offset, RegisterOffset(longs = 1))
    val nameOffset    = offset
    offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
    val ageOffset     = offset
    offset = RegisterOffset.add(offset, RegisterOffset(ints = 1))
    val addressOffset = offset
    offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
    new CachedOffsets(idOffset, nameOffset, ageOffset, addressOffset)
  }

  def encode(p: Person): (Long, AnyRef, Int, AnyRef) = {
    val reflect       = RegistersDomain.Person.blocksSchema.reflect
    val record        = reflect.asRecord.get
    val recordBinding =
      try record.recordBinding.asInstanceOf[Binding.Record[Person]]
      catch {
        case _: Exception =>
          record.recordBinding
            .asInstanceOf[BindingInstance[DdbCodec, ?, Person]]
            .binding
            .asInstanceOf[Binding.Record[Person]]
      }

    // encode
    val co = cachedOffsets

    val registers     = Registers(record.usedRegisters)
    val deconstructor = recordBinding.deconstructor
    deconstructor.deconstruct(registers, RegisterOffset.Zero, p)
    val id            = registers.getLong(co.id, 0)
    val name          = registers.getObject(co.name, 0)
    val age           = registers.getInt(co.age, 0)
    val address       = registers.getObject(co.address, 0)
    (id, name, age, address)
  }

  def decode(p: Person): Unit = {
    val reflect       = RegistersDomain.Person.blocksSchema.reflect
    val record        = reflect.asRecord.get
    val recordBinding =
      try record.recordBinding.asInstanceOf[Binding.Record[Person]]
      catch {
        case _: Exception =>
          record.recordBinding
            .asInstanceOf[BindingInstance[DdbCodec, ?, Person]]
            .binding
            .asInstanceOf[Binding.Record[Person]]
      }

    // encode
    val registers     = Registers(record.usedRegisters)
    val deconstructor = recordBinding.deconstructor
    deconstructor.deconstruct(registers, RegisterOffset.Zero, p)
    registers.getLong(cachedOffsets.id, 0)
    registers.getObject(cachedOffsets.name, 0)
    registers.getInt(cachedOffsets.age, 0)
    registers.getObject(cachedOffsets.address, 0)
    ()
  }

}

object ExerciseRegistersThreadLocal {
  final class CachedOffsets(
    val id: RegisterOffset,
    val name: RegisterOffset,
    val age: RegisterOffset,
    val address: RegisterOffset
  )

  private val threadLocal = new ThreadLocal[CachedOffsets] {
    override def initialValue(): CachedOffsets = {
      // compute offsets per-thread (mirrors production)
      var offset        = RegisterOffset.Zero
      val idOffset      = offset
      offset = RegisterOffset.add(offset, RegisterOffset(longs = 1))
      val nameOffset    = offset
      offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
      val ageOffset     = offset
      offset = RegisterOffset.add(offset, RegisterOffset(ints = 1))
      val addressOffset = offset
      offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
      new CachedOffsets(idOffset, nameOffset, ageOffset, addressOffset)
    }
  }

  def encode(p: Person): (Long, AnyRef, Int, AnyRef) = {
    // reflect/recordBinding in method as requested
    val reflect       = RegistersDomain.Person.blocksSchema.reflect
    val record        = reflect.asRecord.get
    val recordBinding =
      try record.recordBinding.asInstanceOf[Binding.Record[Person]]
      catch {
        case _: Exception =>
          record.recordBinding
            .asInstanceOf[BindingInstance[DdbCodec, ?, Person]]
            .binding
            .asInstanceOf[Binding.Record[Person]]
      }

    val co            = threadLocal.get() // fast per-thread
    val registers     = Registers(record.usedRegisters)
    val deconstructor = recordBinding.deconstructor
    deconstructor.deconstruct(registers, RegisterOffset.Zero, p)

    val id      = registers.getLong(co.id, 0)
    val name    = registers.getObject(co.name, 0)
    val age     = registers.getInt(co.age, 0)
    val address = registers.getObject(co.address, 0)
    (id, name, age, address)
  }
}

object RegistersDomain {
  sealed trait PaymentMethod
  object PaymentMethod extends CompanionOptics[PaymentMethod] {
    case class CreditCard(name: String, cvv: Int) extends PaymentMethod
    object CreditCard {
      implicit val blocksSchema: Schema[CreditCard] = Schema.derived
    }
    case object DebitCard extends PaymentMethod
    case object Paypal extends PaymentMethod

    implicit val blocksSchema: Schema[PaymentMethod] = Schema.derived
  }
  case class Person(
    id: Long,
    name: String,
    age: Int,
    address: String
  )
  object Person {
    implicit val blocksSchema: Schema[Person] = Schema.derived
  }

  val zioBlocksCodec: DdbCodec[Person] = Schema.derived.deriving(BlocksDdbDerived2).derive
}
