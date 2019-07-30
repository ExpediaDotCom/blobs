package com.expedia.www.haystack.agent.blobs.server.spi

import java.io.IOException
import java.net.URL
import java.util
import java.util.concurrent.TimeUnit

import com.expedia.www.haystack.agent.blobs.dispatcher.core.BlobDispatcher
import com.typesafe.config.ConfigFactory
import io.grpc.Server
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, Matchers}

class BlobAgentSpec extends FunSpec with Matchers with EasyMockSugar {

  private val dispatcherLoadFile = "META-INF/services/com.expedia.www.haystack.agent.blobs.dispatcher.core.BlobDispatcher"

  private val server = new Server {

    var isShutdownCalled: Boolean = false

    override def start(): Server = ???

    override def shutdown(): Server = {
      isShutdownCalled = true
      return this
    }

    override def shutdownNow(): Server = ???

    override def isShutdown: Boolean = isShutdownCalled

    override def isTerminated: Boolean = ???

    override def awaitTermination(l: Long, timeUnit: TimeUnit): Boolean = ???

    override def awaitTermination(): Unit = ???
  }

  describe("Blob Agent") {
    it("should return the 'ossblobs' as name") {
      new BlobAgent().getName shouldEqual "ossblobs"
    }

    it("should load the dispatchers from the config") {
      val agent = new BlobAgent()
      val cfg = ConfigFactory.parseString(
        """
          |    port = 34001
          |
          |    dispatchers {
          |      test-dispatcher {
          |        bucket.name = "mybucket"
          |        region = "us-east-1"
          |      }
          |    }
        """.stripMargin)

      val cl = new ReplacingClassLoader(getClass.getClassLoader, dispatcherLoadFile, "META-INF/services/dispatcherProvider.txt")
      val dispatchers = agent.loadAndInitializeDispatchers(cfg, cl)
      dispatchers.size() shouldBe 1
      dispatchers.get(0).close()
    }
  }

  it("initialization should fail if no dispatchers exist") {
    val agent = new BlobAgent()
    val cfg = ConfigFactory.parseString(
      """
        |    port = 34001
        |
        |    dispatchers {
        |      test-dispatcher {
        |        bucket.name = "mybucket"
        |        region = "us-east-1"
        |      }
        |    }
      """.stripMargin)

    val caught = intercept[Exception] {
      agent.loadAndInitializeDispatchers(cfg, getClass.getClassLoader)
    }

    caught.getMessage shouldEqual "Blob agent dispatchers can't be an empty set"
  }

  it("should fail if maximum blob size is not present in config") {
    val agent = new BlobAgent()
    val cfg = ConfigFactory.parseString(
      """
        |    port = 34001
        |
        |    dispatchers {
        |      test-dispatcher {
        |        bucket.name = "mybucket"
        |        region = "us-east-1"
        |      }
        |    }
      """.stripMargin)
    val dispatchers = mock[util.ArrayList[BlobDispatcher]]

    val caught = intercept[Exception] {
      agent._initialize(dispatchers, cfg)
    }

    caught.getMessage shouldEqual "max message size for blobs needs to be specified"
  }

  it("should fail if port is not present in config") {
    val agent = new BlobAgent()
    val cfg = ConfigFactory.parseString(
      """
        |    max.blob.size.in.kb = 50
        |
        |    dispatchers {
        |      test-dispatcher {
        |        bucket.name = "mybucket"
        |        region = "us-east-1"
        |      }
        |    }
      """.stripMargin)
    val dispatchers = mock[util.ArrayList[BlobDispatcher]]

    val caught = intercept[Exception] {
      agent._initialize(dispatchers, cfg)
    }

    caught.getMessage shouldEqual "port for service needs to be specified"
  }

  it("should close all the dispatchers and the server if agent's close is called") {
    val firstDispatcher = mock[BlobDispatcher]
    val secondDispatcher = mock[BlobDispatcher]

    val dispatchers = new util.ArrayList[BlobDispatcher](2)
    dispatchers.add(firstDispatcher)
    dispatchers.add(secondDispatcher)

    val agent = new BlobAgent(dispatchers, server)

    expecting {
      firstDispatcher.close()
      secondDispatcher.close()
    }

    whenExecuting(firstDispatcher, secondDispatcher) {
      agent.close()
      server.isShutdownCalled shouldEqual true
    }

  }

  class ReplacingClassLoader(val parent: ClassLoader, val resource: String, val replacement: String) extends ClassLoader(parent) {
    override def getResource(name: String): URL = {
      if (resource == name) {
        return getParent.getResource(replacement)
      }
      super.getResource(name)
    }

    @throws[IOException]
    override def getResources(name: String): util.Enumeration[URL] = {
      if (resource == name) {
        return getParent.getResources(replacement)
      }
      super.getResources(name)
    }
  }

}
