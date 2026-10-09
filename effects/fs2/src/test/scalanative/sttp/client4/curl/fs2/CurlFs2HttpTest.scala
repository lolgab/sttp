package sttp.client4.curl.fs2

import cats.effect.IO
import sttp.client4.Backend
import sttp.client4.curl.cats.CurlHttpTestOverrides
import sttp.client4.impl.cats.{CatsRetryTest, CatsTestBase}
import sttp.client4.testing.HttpTest

class CurlFs2HttpTest extends HttpTest[IO] with CatsTestBase with CatsRetryTest with CurlHttpTestOverrides {
  // released when the process ends
  override implicit val backend: Backend[IO] = CurlFs2Backend.resource().allocated.map(_._1).unsafeRunSync()
  override def supportsHostHeaderOverride = false
  override def supportsDeflateWrapperChecking = false
  override def supportsCancellation = false
}
