package zio.dynamodb

import zio.dynamodb.Zipped.Zippable

object ZippedExample extends App {

  final case class Task[+A](unsafeRun: () => A) { self =>
    def ~[B](that: Task[B])(implicit z: Zippable[A, B]): Task[z.Out] = self zip that

    def flatMap[B](f: A => Task[B]): Task[B] = Task(() => f(unsafeRun()).unsafeRun())

    def map[B](f: A => B): Task[B] = self.flatMap(a => Task.attempt(f(a)))

    def zip[B](that: Task[B])(implicit z: Zippable[A, B]): Task[z.Out] =
      self.flatMap(a => that.map(b => z.zip(a, b)))
  }
  object Task                                   {
    def attempt[A](a: => A): Task[A] = Task(() => a)
  }

  val task: Task[String] = Task.attempt("foo")

  val result1: Task[(String, String)] = task.zip(task)

  val result2: Task[(String, String, String, String, String)] =
    task.zip(task).zip(task).zip(task).zip(task)

  val result3: Task[(String, String, String, String, String)] =
    task ~ task ~ task ~ task ~ task

  def myZip[A, B](t1: Task[A], t2: Task[B]) =
    t1.zip(t2)

  // (fa |@| fb |@| fc)(f(_, _, _))
  // (fa, fb, fc).mapN(f(_, _, _))
  // ZIO.mapN(fa, fb, fc)(f(_, _, _))
  // Operator to zip / custom extractor
  // Custom flatten operator

}
