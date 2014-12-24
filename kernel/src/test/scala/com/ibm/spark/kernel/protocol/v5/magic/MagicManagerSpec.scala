/*
 * Copyright 2014 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.spark.kernel.protocol.v5.magic

import java.io.OutputStream

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.ibm.spark.interpreter.{ExecuteOutput, ExecuteError}
import com.ibm.spark.magic.{MagicTemplate, MagicLoader}
import com.typesafe.config.ConfigFactory
import org.mockito.Matchers.{eq => mockEq, _}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.duration._

object MagicManagerSpec {
  val config = """
    akka {
      loglevel = "WARNING"
    }"""
}

class MagicManagerSpec extends TestKit(
  ActorSystem(
    "MagicManagerSpec",
    ConfigFactory.parseString(MagicManagerSpec.config)
  )
) with ImplicitSender with FunSpecLike with Matchers with MockitoSugar {
  describe("MagicManager") {
    describe("#receive") {
      describe("with message type ValidateMagicMessage") {
        it("should return false if the code does not parse as magic") {
          val mockMagicLoader = mock[MagicLoader]
          val magicManager =
            system.actorOf(Props(classOf[MagicManager], mockMagicLoader))

          magicManager ! ValidateMagicMessage("notAMagic")

          expectMsg(200.milliseconds, false)
        }

        it("should return true if code parses as line magic") {
          val mockMagicLoader = mock[MagicLoader]
          val magicManager =
            system.actorOf(Props(classOf[MagicManager], mockMagicLoader))

          magicManager ! ValidateMagicMessage("%lineMagic asdfasdf")

          expectMsg(200.milliseconds, true)
        }

        it("should return true if code parses as cell magic") {
          val mockMagicLoader = mock[MagicLoader]
          val magicManager =
            system.actorOf(Props(classOf[MagicManager], mockMagicLoader))

          magicManager ! ValidateMagicMessage("%%cellMagic asdflj\nasdf\n")

          expectMsg(200.milliseconds, true)
        }
      }

      describe("with message type (ExecuteMagicMessage, OutputStream)") {
        it("should return an error if the magic requested is not defined") {
          val fakeMagicName = "myMagic"
          val mockMagicLoader = mock[MagicLoader]
          doReturn(false).when(mockMagicLoader).hasMagic(anyString())
          val magicManager =
            system.actorOf(Props(classOf[MagicManager], mockMagicLoader))

          magicManager ! ((
            ExecuteMagicMessage("%%" + fakeMagicName),
            mock[OutputStream]
          ))

          // Expect magic to not exist
          expectMsg(200.milliseconds, Right(ExecuteError(
            "Missing Magic",
            s"Magic $fakeMagicName does not exist!",
            List()
          )))
        }

        it("should evaluate the magic if it exists and return the error if it fails") {
          val fakeMagicName = "myBadMagic"
          val fakeMagicReturn = new RuntimeException("EXPLOSION")

          val mockMagic = mock[MagicTemplate]
          doThrow(fakeMagicReturn).when(mockMagic).executeCell(any[Seq[String]])
          val myMagicLoader = new MagicLoader() {
            override def createMagicInstance(name: String) =
              mockMagic
          }
          val magicManager =
            system.actorOf(Props(classOf[MagicManager], myMagicLoader))

          magicManager ! ((
            ExecuteMagicMessage("%%" + fakeMagicName),
            mock[OutputStream]
          ))

          val result =
            receiveOne(5.seconds)
              .asInstanceOf[Either[ExecuteOutput, ExecuteError]]

          result.right.get shouldBe an [ExecuteError]
        }

        it("should evaluate the magic if it exists and return the output if it succeeds") {
          val fakeMagicName = "myMagic"
          val fakeMagicReturn = Map()

          val mockMagic = mock[MagicTemplate]
          doReturn(fakeMagicReturn)
            .when(mockMagic).executeCell(any[Seq[String]])

          val myMagicLoader = new MagicLoader() {
            override def hasMagic(name: String): Boolean = true

            override def createMagicInstance(name: String) =
              mockMagic
          }

          val magicManager =
            system.actorOf(Props(classOf[MagicManager], myMagicLoader))

          magicManager ! ((
            ExecuteMagicMessage("%%" + fakeMagicName),
            mock[OutputStream]
          ))

          // TODO: Refactor timeout-based test to avoid incremental adjustment
          expectMsg(3000.milliseconds, Left(fakeMagicReturn))
        }
      }
    }
  }
}