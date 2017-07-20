package io.vertx.ext.coroutine

import io.vertx.core.Vertx
import io.vertx.core.VertxException
import io.vertx.core.eventbus.Message
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpServerOptions
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.withTimeout
import org.hamcrest.core.Is
import org.junit.Assert.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by stream.
 */
class TestVerticle : CoroutineVerticle() {

  private val ADDRESS1 = "address1"
  private val ADDRESS2 = "address2"
  private val ADDRESS3 = "address3"

  private lateinit var ai: AsyncInterface
  private lateinit var completeChannel: Channel<Any>

  @Throws(Exception::class)
  override fun start() {
    ai = AsyncInterfaceImpl(vertx)
    completeChannel = Channel(1)
    try {
      val testName = config().getString("testName")
      val method = this::class.java.getDeclaredMethod(testName)
      method.isAccessible = true
      method.invoke(this)

    } catch (e: AssertionError) {
      e.printStackTrace()
      throw IllegalStateException("Tests failed", e)
    } catch (e: Exception) {
      e.printStackTrace()
      throw IllegalStateException("Failed to invoke test", e)
    }
    runVertxCoroutine {
      withTimeout(10, TimeUnit.SECONDS) {
        completeChannel.receive()
      }
    }
  }

  fun complete() {
    try {
      runVertxCoroutine {
        completeChannel.send(Any())
      }
    } catch (e: Exception) {
      throw VertxException(e)
    }

  }

  fun testContext() {
    val ctx = Vertx.currentContext()
    assertTrue(ctx.isEventLoopContext)
    complete()
  }

  @Throws(Exception::class)
  fun testSleep() = runVertxCoroutine {
    val th = Thread.currentThread()
    val cnt = AtomicInteger()
    val periodicTimer = vertx.periodicStream(1).handler {
      runVertxCoroutine {
        assertSame(Thread.currentThread(), th)
        cnt.incrementAndGet()
      }
    }
    asyncEvent<Long> { h -> vertx.setTimer(1000L, h) }.await()
    assertSame(Thread.currentThread(), th)
    assertTrue(cnt.get() > 900)
    periodicTimer.cancel()
    complete()
  }

  fun testFiberHandler() {
    val server = vertx.createHttpServer(HttpServerOptions().setPort(8080))
    server.requestHandler({ req ->
      runVertxCoroutine {
        val res = asyncResult<String> { h -> ai.methodWithParamsAndHandlerNoReturn("oranges", 23, h) }.await()
        assertEquals("oranges23", res)
        req.response().end()
      }
    })
    server.listen { res ->
      assertTrue(res.succeeded())
      val client = vertx.createHttpClient(HttpClientOptions().setDefaultPort(8080))
      client.getNow("/somepath") { resp ->
        assertTrue(resp.statusCode() == 200)
        client.close()
        server.close { _ -> complete() }
      }
    }
  }

  fun testExecSyncMethodWithParamsAndHandlerNoReturn() {
    val th = Thread.currentThread()
    runVertxCoroutine {
      val res = asyncResult<String> { h -> ai.methodWithParamsAndHandlerNoReturn("oranges", 23, h) }.await()
      assertEquals("oranges23", res)
      assertSame(Thread.currentThread(), th)
      complete()
    }
  }

  fun testExecSyncMethodWithNoParamsAndHandlerNoReturn() = runVertxCoroutine {
    val res = asyncResult<String> { h -> ai.methodWithNoParamsAndHandlerNoReturn(h) }.await()
    assertEquals("wibble", res)
    complete()
  }

  fun testExecSyncMethodWithParamsAndHandlerWithReturn() = runVertxCoroutine {
    val res = asyncResult<String> { h -> ai.methodWithParamsAndHandlerWithReturn("oranges", 23, h) }.await()
    assertEquals("oranges23", res)
    complete()
  }

  fun testExecSyncMethodWithNoParamsAndHandlerWithReturn() = runVertxCoroutine {
    val res = asyncResult<String> { h -> ai.methodWithNoParamsAndHandlerWithReturn(h) }.await()
    assertEquals("wibble", res)
    complete()
  }

  fun testExecSyncMethodWithNoParamsAndHandlerWithReturnNoTimeout() = runVertxCoroutine {
    val res = asyncResult<String>(2000) { h -> ai.methodWithNoParamsAndHandlerWithReturnTimeout(h, 1000) }.await()
    assertEquals("wibble", res)
    complete()
  }

  fun testExecSyncMethodWithNoParamsAndHandlerWithReturnTimedout() = runVertxCoroutine {
    val res = asyncResult<String>(500) { h -> ai.methodWithNoParamsAndHandlerWithReturnTimeout(h, 1000) }.await()
    assertNull(res)
    complete()
  }

  fun testExecSyncMethodWithParamsAndHandlerInterface() = runVertxCoroutine {
    val returned = asyncResult<ReturnedInterface> { h -> ai.methodWithParamsAndHandlerInterface("apples", 123, h) }.await()
    assertNotNull(returned)
    val res = asyncResult<String> { h -> returned.methodWithParamsAndHandlerNoReturn("bananas", 100, h) }.await()
    assertEquals(res, "bananas100")
    complete()
  }

  fun testExecSyncMethodThatFails() = runVertxCoroutine {
    try {
      val res = asyncResult<String> { h -> ai.methodThatFails("oranges", h) }.await()
      fail("Should throw exception")
    } catch (e: Exception) {
      assertTrue(e is VertxException)
      val ve = e as VertxException
      assertEquals("oranges", ve.cause?.message)
      complete()
    }

  }

  fun testReceiveEvent() = runVertxCoroutine {
    val start = System.currentTimeMillis()
    val tid = asyncEvent<Long> { h -> vertx.setTimer(500, h) }.await()
    val end = System.currentTimeMillis()
    assertTrue(end - start >= 500)
    assertTrue(tid >= 0)
    complete()
  }

  fun testReceiveEventTimedout() = runVertxCoroutine {
    val start = System.currentTimeMillis()
    try {
      val tid = asyncEvent<Long>(250) { h -> vertx.setTimer(500, h) }
    } catch (npe: NullPointerException) {
      assertThat<NullPointerException>(npe, Is.isA<NullPointerException>(NullPointerException::class.java))
    } catch (e: Exception) {
      assertTrue(false)
    } finally {
      complete()
    }
  }

  fun testReceiveEventNoTimeout() = runVertxCoroutine {
    val start = System.currentTimeMillis()
    val tid = asyncEvent<Long>(1000L) { h -> vertx.setTimer(500, h) }.await()
    val end = System.currentTimeMillis()
    assertTrue(end - start >= 500)
    assertTrue(tid >= 0)
    complete()
  }

  @Throws(Exception::class)
  fun testHandlerAdaptor() = runVertxCoroutine {
    val eb = vertx.eventBus()

    // Create a couple of consumers on different addresses
    // The adaptor allows handler to be used as a Channel
    val adaptor1 = streamAdaptor<Message<String>>()
    eb.consumer<String>(ADDRESS1).handler(adaptor1)

    val adaptor2 = streamAdaptor<Message<String>>()
    eb.consumer<String>(ADDRESS2).handler(adaptor2)

    // Set up a periodic timer to send messages to these addresses

    val start = System.currentTimeMillis()

    vertx.setPeriodic(10) { tid ->
      eb.send(ADDRESS1, "wibble")
      eb.send(ADDRESS2, "flibble")
    }

    for (i in 0..9) {

      val received1 = adaptor1.receive()
      assertEquals("wibble", received1.body())

      val received2 = adaptor2.receive()
      assertEquals("flibble", received2.body())

    }

    val end = System.currentTimeMillis()
    assertTrue(end - start >= 100)

    // Try a receive with timeout
    var received1 = adaptor1.receive(1000)
    assertEquals("wibble", received1.body())


    // And timing out
    val adaptor3 = streamAdaptor<Message<String>>()
    eb.consumer<String>(ADDRESS3).handler(adaptor3)
    val received3 = adaptor3.receive(100)
    assertNull(received3)

    // Try underlying channel
    val channel = adaptor1.channel
    assertNotNull(channel)
    received1 = channel.receive()
    assertEquals("wibble", received1.body())

    complete()
  }

  override fun stop() {
    try {
      testContext()
    } catch (e: AssertionError) {
      e.printStackTrace()
      fail("tests failed")
    }
  }
}