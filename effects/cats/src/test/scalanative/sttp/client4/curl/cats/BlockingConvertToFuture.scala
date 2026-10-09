package sttp.client4.curl.cats

import cats.effect.IO
import sttp.client4.impl.cats.CatsTestBase
import sttp.client4.testing.ConvertToFuture

import scala.concurrent.duration.Duration
import scala.concurrent.{CanAwait, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** On Scala Native, ScalaTest doesn't look at the result of tests which return a `Future`: an assertion which fails
  * inside of a `Future` doesn't fail the test, and the test doesn't even wait for the future to complete. To get
  * meaningful results, the `IO` is run to completion before the test continues, and the returned future runs the
  * functions given to `map`/`flatMap` synchronously, letting the exceptions (e.g. failed assertions) propagate to the
  * test.
  */
trait BlockingConvertToFuture extends CatsTestBase {
  override implicit val convertToFuture: ConvertToFuture[IO] = new ConvertToFuture[IO] {
    override def toFuture[T](value: IO[T]): Future[T] =
      new SynchronousFuture(Try(value.unsafeRunSync()(ioRuntime)))
  }
}

private final class SynchronousFuture[T](outcome: Try[T]) extends Future[T] {
  override def onComplete[U](f: Try[T] => U)(implicit executor: ExecutionContext): Unit = { f(outcome); () }
  override def isCompleted: Boolean = true
  override def value: Option[Try[T]] = Some(outcome)

  override def transform[S](f: Try[T] => Try[S])(implicit executor: ExecutionContext): Future[S] =
    new SynchronousFuture(f(outcome))
  override def transformWith[S](f: Try[T] => Future[S])(implicit executor: ExecutionContext): Future[S] = f(outcome)

  // unlike the default implementations, these don't catch the exceptions thrown by `f`
  override def map[S](f: T => S)(implicit executor: ExecutionContext): Future[S] = outcome match {
    case Success(v) => new SynchronousFuture(Success(f(v)))
    case Failure(e) => new SynchronousFuture(Failure(e))
  }
  override def flatMap[S](f: T => Future[S])(implicit executor: ExecutionContext): Future[S] = outcome match {
    case Success(v) => f(v)
    case Failure(e) => new SynchronousFuture(Failure(e))
  }

  override def ready(atMost: Duration)(implicit permit: CanAwait): this.type = this
  override def result(atMost: Duration)(implicit permit: CanAwait): T = outcome.get
}
