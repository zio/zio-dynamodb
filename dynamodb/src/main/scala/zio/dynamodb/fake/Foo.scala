//package zio.dynamodb.fake
//
//import zio.dynamodb.fake.Dec.Dec
//import zio.dynamodb.fake.Inc.Inc
//import zio.{ Has, Ref, UIO, ULayer, ZIO, ZLayer }
//
//object Inc {
//  type Inc = Has[Service]
//
//  trait Service {
//    def get(i: Int): UIO[Int]
//  }
//}
//object Dec {
//
//  type Dec = Has[Service]
//
//  trait Service {
//    def set(i: Int): UIO[Unit]
//  }
//}
//
//final case class Foo(ref: Ref[Int]) extends Inc.Service with Dec.Service {
//  override def get(i: Int): UIO[Int]  = ref.get
//  override def set(i: Int): UIO[Unit] = ref.set(i)
//}
//
//object FooLayer {
//  // what I want is below layer using a single Foo instance
//  val layer: ULayer[Inc with Dec] = {
//    val value: ZIO[Any, Nothing, Has[Inc.Service] with Has[Dec.Service]]     =
//      Ref.make[Int](0).map(Foo(_)).map(foo => Has[Inc.Service](foo) ++ Has[Dec.Service](foo))
//    val x: ZLayer[Any, Nothing, Has[Has[Inc.Service] with Has[Dec.Service]]] = value.toLayer
//    value.toLayerMany
//  }
//
//}
//
///*
//  val x: ZIO[Any, Nothing, Has[Inc.Service] with Has[Dec.Service]] = (for {
//    ref <- Ref.make(0)
//    x    = Foo(ref)
//  } yield Has.allOf[Inc.Service, Dec.Service](x, x))
//
// */
