package io.vertx.ext.coroutine

import io.vertx.core.DeploymentOptions
import io.vertx.core.json.JsonObject
import io.vertx.test.core.VertxTestBase
import org.junit.Test

/**
 * Created by stream.
 */
class VertxCoroutineTest : VertxTestBase() {

  @Throws(Exception::class)
  internal fun runTest(testName: String) {
    vertx.deployVerticle(TestVerticle(),
      DeploymentOptions().setConfig(JsonObject().put("testName", testName))) { res ->
      if (res.succeeded()) {
        vertx.undeploy(res.result()) { res2 ->
          if (res2.succeeded()) {
            testComplete()
          } else {
            res2.cause().printStackTrace()
            fail("Failure in undeploying")
          }
        }
      } else {
        res.cause().printStackTrace()
        fail("Failure in running tests")
      }
    }
    await()
  }

  internal fun getMethodName(): String {
    return Thread.currentThread().stackTrace[2].methodName
  }

  // Test fiber handler

  @Test
  @Throws(Exception::class)
  fun testFiberHandler() {
    runTest(getMethodName())
  }

  // Test exec sync

  @Test
  @Throws(Exception::class)
  fun testExecSyncMethodWithParamsAndHandlerNoReturn() {
    runTest(getMethodName())
  }

  @Test
  @Throws(Exception::class)
  fun testExecSyncMethodWithNoParamsAndHandlerNoReturn() {
    runTest(getMethodName())
  }

  @Test
  @Throws(Exception::class)
  fun testExecSyncMethodWithParamsAndHandlerWithReturn() {
    runTest(getMethodName())
  }

  @Test
  @Throws(Exception::class)
  fun testExecSyncMethodWithNoParamsAndHandlerWithReturn() {
    runTest(getMethodName())
  }

  @Test
  @Throws(Exception::class)
  fun testExecSyncMethodWithParamsAndHandlerInterface() {
    runTest(getMethodName())
  }

  @Test
  @Throws(Exception::class)
  fun testExecSyncMethodWithNoParamsAndHandlerWithReturnNoTimeout() {
    runTest(getMethodName())
  }

  @Test
  @Throws(Exception::class)
  fun testExecSyncMethodWithNoParamsAndHandlerWithReturnTimedout() {
    runTest(getMethodName())
  }

  @Test
  @Throws(Exception::class)
  fun testExecSyncMethodThatFails() {
    runTest(getMethodName())
  }

  // Test receive single event

  @Test
  @Throws(Exception::class)
  fun testReceiveEvent() {
    runTest(getMethodName())
  }

  @Test
  @Throws(Exception::class)
  fun testReceiveEventTimedout() {
    runTest(getMethodName())
  }

  @Test
  @Throws(Exception::class)
  fun testReceiveEventNoTimeout() {
    runTest(getMethodName())
  }

  // Test Channels
  @Test
  @Throws(Exception::class)
  fun testHandlerAdaptor() {
    runTest(getMethodName())
  }

  // Various misc

  @Test
  @Throws(Exception::class)
  fun testSleep() {
    runTest(getMethodName())
  }
}
