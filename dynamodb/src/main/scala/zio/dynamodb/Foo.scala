package zio.dynamodb

/*
ideas:
- make `where` return a Write query + PhantomType
 */
object FooIntersectionTypesWorks {
  trait HasNoCondition
  trait Query[+A, -R] {
    def execute: Query[A, R] = ???
  }
  final case class PutWithCondition(id: String, sku: String) extends Query[PutWithCondition, String]
  final case class Put(id: String, sku: String) extends Query[PutWithCondition, String] with HasNoCondition

  val x: PutWithCondition = ???
  val y: Put              = ???

  def foo(x: Query[_, _] with HasNoCondition): Unit = ???

  /*
foo(x) gives compile error
type mismatch;
 found   : zio.dynamodb.FooIntersectionTypesWorks.x.type (with underlying type zio.dynamodb.FooIntersectionTypesWorks.PutWithCondition)
 required: zio.dynamodb.FooIntersectionTypesWorks.Query[_, _] with zio.dynamodb.FooIntersectionTypesWorks.HasNoConditionbloop
   */
  foo(y)
}

object FooPhantomDoesNotWork {
  trait Batchable[X]
  
  trait HasNoCondition
  trait HasCondition
  trait Query[+A, -R] {
    type Condition
    def execute: Query[A, R]                                                     = ???
    def where(condition: String)(implicit ev: Batchable[Condition]): Query[A, R] = ???
  }
  final case class Query1(id: String, sku: String) extends Query[Query1, String]
  final case class Query2(id: String, sku: String) extends Query[Query1, String]

  val x: Query[String, String] = new Query[String, String] {
    type Condition = HasNoCondition
  }

  implicit val batchable: Batchable[HasNoCondition] = new Batchable[HasNoCondition] {}
  //val y = x.where("foo")

  def foo[Q <: Query[_, _]](q: Q): Unit = ???

}

object ExistentialTypes {
  trait Animal {
    def name: String
  }
  class Dog(val name: String) extends Animal
  class Cat(val name: String) extends Animal
  class Storage {
    type T <: Animal
    private var item: Option[Animal] = None
    def put(value: T): Unit = {
      item = Some(value)
      println(s"Stored: ${value.name}")
    }
    def get: Option[Animal]          = item
  }

// Create a Storage instance specifically for Dogs
  val dogStorage = new Storage {
    type T = Dog // Specify T as Dog for this instance
  }
  val catStorage = new Storage {
    type T = Cat // Specify T as Cat for this instance
  }

  dogStorage.put(new Dog("Buddy"))
  catStorage.put(new Cat("Whiskers"))
}

