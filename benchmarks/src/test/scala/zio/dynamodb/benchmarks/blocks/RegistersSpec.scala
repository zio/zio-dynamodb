package zio.dynamodb.benchmarks.blocks

import zio.test.{ assertTrue, ZIOSpecDefault }
import ListOfRecordsDomain._
import zio.blocks.schema.binding.{ Binding, RegisterOffset, Registers }
import zio.blocks.schema.derive.BindingInstance
import zio.dynamodb.blocks.DdbCodec
object RegistersSpec extends ZIOSpecDefault {
  override def spec =
    suite("RegistersSpec")(
      test("placeholder test") {
        val p = Person(
          id = 1L,
          name = "John Doe",
          age = 30,
          address = "123 Main St"
        )

        val reflect       = ListOfRecordsDomain.Person.blocksSchema.reflect
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
        val registers       = Registers(record.usedRegisters)
        val deconstructor   = recordBinding.deconstructor
        deconstructor.deconstruct(registers, RegisterOffset.Zero, p)
        var offset          = RegisterOffset.Zero
        val id: Long        = registers.getLong(offset, 0)
        offset = RegisterOffset.add(offset, RegisterOffset(longs = 1))
        val name: AnyRef    = registers.getObject(offset, 0)
        offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
        val age: Int        = registers.getInt(offset, 0)
        offset = RegisterOffset.add(offset, RegisterOffset(ints = 1))
        val address: AnyRef = registers.getObject(offset, 0)
        offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))

        // decode
        val registers2  = Registers(record.usedRegisters)
        val constructor = recordBinding.constructor
        offset = RegisterOffset.Zero
        registers2.setLong(offset, 0, id)
        offset = RegisterOffset.add(offset, RegisterOffset(longs = 1))
        registers2.setObject(offset, 0, name)
        offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
        registers2.setInt(offset, 0, age)
        offset = RegisterOffset.add(offset, RegisterOffset(ints = 1))
        registers2.setObject(offset, 0, address)
        offset = RegisterOffset.add(offset, RegisterOffset(objects = 1))
        val p2          = constructor.construct(registers2, RegisterOffset.Zero)

        assertTrue(p == p2)
      }
    )

}
