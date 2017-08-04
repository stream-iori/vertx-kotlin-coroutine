package io.vertx.kotlin.coroutines

import io.vertx.core.Vertx
import io.vertx.core.http.HttpClientResponse
import io.vertx.core.http.HttpServer
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Created by stream.
 */
@RunWith(VertxUnitRunner::class)
class HelloWorldTest {

  @Rule
  @JvmField val rule = RunTestOnContext()
  private lateinit var vertx: Vertx
  private lateinit var httpServer: HttpServer

  @Before
  fun setUp(context: TestContext) {
    vertx = rule.vertx()
    val async = context.async()
    httpServer = vertx.createHttpServer().requestHandler { req -> req.response().end("Hello, World!") }.listen(8080, { result ->
      if (result.succeeded()) async.complete()
      else context.fail("failed.")
    })
  }

  @After
  fun tearDown(context: TestContext) {
    val async = context.async()
    httpServer.close({ result ->
      if (result.succeeded()) async.complete()
      else context.fail("failed.")
    })
  }


  @Test
  fun testAsync(context: TestContext) {
    val atc = context.async()
    vertx.createHttpClient().getNow(8080, "localhost", "/") { response ->
      response.handler { body ->
        context.assertTrue(body.toString() == "Hello, World!")
        atc.complete()
      }
    }
  }


  @Test
  fun testSync(context: TestContext) {
    val atc = context.async()
    runVertxCoroutine {
      val response = asyncEvent<HttpClientResponse> { vertx.createHttpClient().getNow(8080, "localhost", "/", it) }.await()
      response.handler { body ->
        context.assertTrue(body.toString() == "Hello, World!")
        atc.complete()
      }
    }
  }

}
