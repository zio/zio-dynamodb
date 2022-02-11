//package zio.dynamodb
//
//import scala.annotation.implicitNotFound
//
//@implicitNotFound("Could not find an implicit C[${T}, ${U}]")
//sealed trait C[-T, -U]
//
//class K[A] {
//  def m[B](implicit c: C[List[A], B])                                                                      = 0
//  def n[B](implicit @implicitNotFound("Specific message for C of list of ${A} and ${B}") c: C[List[A], B]) = 1
//}
//
//object Test {
//  val k = new K[Int]
//  k.m[String]
//  k.n[String]
//}
