package com.ibm.spark.kernel.protocol.v5.comm

import java.util.UUID
import com.ibm.spark.kernel.protocol.v5._
import com.ibm.spark.kernel.protocol.v5.content._
import play.api.libs.json.Json
import scala.concurrent.duration._

import akka.actor.{ActorSelection, ActorSystem}
import akka.testkit.{TestProbe, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSpecLike, Matchers}
import org.mockito.Mockito._
import org.mockito.Matchers._

object CommWriterSpec {
  val config ="""
    akka {
      loglevel = "WARNING"
    }"""
}

class CommWriterSpec extends TestKit(
  ActorSystem("CommWriterSpec",
    ConfigFactory.parseString(CommWriterSpec.config))
) with FunSpecLike with Matchers with BeforeAndAfter with MockitoSugar
{

  private val commId = UUID.randomUUID().toString
  private var commWriter: CommWriter = _
  private var kernelMessageBuilder: KMBuilder = _

  private var actorLoader: ActorLoader = _
  private var kernelMessageRelayProbe: TestProbe = _

  /**
   * Retrieves the next message available.
   *
   * @return The KernelMessage instance (or an error if timed out)
   */
  private def getNextMessage =
    kernelMessageRelayProbe.receiveOne(200.milliseconds)
      .asInstanceOf[KernelMessage]

  /**
   * Retrieves the next message available and returns its type.
   *
   * @return The type of the message (pulled from message header)
   */
  private def getNextMessageType =
    MessageType.withName(getNextMessage.header.msg_type)

  /**
   * Retrieves the next message available and parses the content string.
   *
   * @tparam T The type to coerce the content string into
   *
   * @return The resulting KernelMessageContent instance
   */
  private def getNextMessageContents[T <: KernelMessageContent]
    (implicit fjs: play.api.libs.json.Reads[T], mf: Manifest[T]) =
  {
    val receivedMessage = getNextMessage

    Json.parse(receivedMessage.contentString).as[T]
  }

  before {
    kernelMessageBuilder = spy(KMBuilder())

    // Construct path for kernel message relay
    actorLoader = mock[ActorLoader]
    kernelMessageRelayProbe = TestProbe()
    val kernelMessageRelaySelection: ActorSelection =
      system.actorSelection(kernelMessageRelayProbe.ref.path.toString)
    doReturn(kernelMessageRelaySelection)
      .when(actorLoader).load(SystemActorType.KernelMessageRelay)

    // Create a new writer to use for testing
    commWriter = new CommWriter(actorLoader, kernelMessageBuilder, commId)
  }

  describe("CommWriter") {
    describe("#writeOpen") {
      it("should send a comm_open message to the relay") {
        commWriter.writeOpen(anyString())

        getNextMessageType should be (MessageType.CommOpen)
      }

      it("should include the comm_id in the message") {
        val expected = commId
        commWriter.writeOpen(anyString())

        val actual = getNextMessageContents[CommOpen].comm_id

        actual should be (expected)
      }

      it("should include the target name in the message") {
        val expected = "<TARGET_NAME>"
        commWriter.writeOpen(expected)

        val actual = getNextMessageContents[CommOpen].target_name

        actual should be (expected)
      }

      it("should provide empty data in the message if no data is provided") {
        val expected = Data()
        commWriter.writeOpen(anyString())

        val actual = getNextMessageContents[CommOpen].data

        actual should be (expected)
      }

      it("should include the data in the message") {
        val expected = Data("some key" -> "some value")
        commWriter.writeOpen(anyString(), expected)

        val actual = getNextMessageContents[CommOpen].data

        actual should be (expected)
      }
    }

    describe("#writeMsg") {
      it("should send a comm_msg message to the relay") {
        commWriter.writeMsg(Data())

        getNextMessageType should be (MessageType.CommMsg)
      }

      it("should include the comm_id in the message") {
        val expected = commId
        commWriter.writeMsg(Data())

        val actual = getNextMessageContents[CommMsg].comm_id

        actual should be (expected)
      }

      it("should fail a require if the data is null") {
        intercept[IllegalArgumentException] {
          commWriter.writeMsg(null)
        }
      }

      it("should include the data in the message") {
        val expected = Data("some key" -> "some value")
        commWriter.writeMsg(expected)

        val actual = getNextMessageContents[CommMsg].data

        actual should be (expected)
      }
    }

    describe("#writeClose") {
      it("should send a comm_close message to the relay") {
        commWriter.writeClose()

        getNextMessageType should be (MessageType.CommClose)
      }

      it("should include the comm_id in the message") {
        val expected = commId
        commWriter.writeClose()

        val actual = getNextMessageContents[CommClose].comm_id

        actual should be (expected)
      }

      it("should provide empty data in the message if no data is provided") {
        val expected = Data()
        commWriter.writeClose()

        val actual = getNextMessageContents[CommClose].data

        actual should be (expected)
      }

      it("should include the data in the message") {
        val expected = Data("some key" -> "some value")
        commWriter.writeClose(expected)

        val actual = getNextMessageContents[CommClose].data

        actual should be (expected)
      }
    }

    describe("#write") {
      it("should send a comm_msg message to the relay") {
        commWriter.write(Array('a'), 0, 1)

        getNextMessageType should be (MessageType.CommMsg)
      }

      it("should include the comm_id in the message") {
        val expected = commId
        commWriter.write(Array('a'), 0, 1)

        val actual = getNextMessageContents[CommMsg].comm_id

        actual should be (expected)
      }

      it("should package the string as part of the data with a 'message' key") {
        val expected = Data("message" -> "a")
        commWriter.write(Array('a'), 0, 1)

        val actual = getNextMessageContents[CommMsg].data

        actual should be (expected)
      }
    }

    describe("#flush") {
      it("should do nothing") {
        // TODO: Is this test necessary? It does nothing.
        commWriter.flush()
      }
    }

    describe("#close") {
      it("should send a comm_close message to the relay") {
        commWriter.close()

        getNextMessageType should be (MessageType.CommClose)
      }

      it("should include the comm_id in the message") {
        val expected = commId
        commWriter.close()

        val actual = getNextMessageContents[CommClose].comm_id

        actual should be (expected)
      }

      it("should provide empty data in the message") {
        val expected = Data()
        commWriter.close()

        val actual = getNextMessageContents[CommClose].data

        actual should be (expected)
      }
    }
  }
}
