package sttp.client4.curl.cats

import cats.effect.IO
import org.scalatest.Assertion
import sttp.client4.Response
import sttp.client4.testing.HttpTest
import sttp.model.StatusCode

import scala.concurrent.Future

/** The adjustments needed to run [[HttpTest]] on Scala Native with the curl backends. */
trait CurlHttpTestOverrides extends HttpTest[IO] with BlockingConvertToFuture {

  // `shouldBe Symbol("empty")` is implemented using reflection, which is not available on Native
  override protected def expectRedirectResponse(response: IO[Response[String]], code: Int): Future[Assertion] =
    response.toFuture().map(resp => (resp.code, resp.history.isEmpty) shouldBe ((StatusCode(code), true)))

  // each part is sent using the charset of the whole body
  override protected def supportsCustomMultipartEncoding = false
}
