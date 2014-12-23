/*
 * Copyright 2014 IBM Corp.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.ibm.spark.kernel.protocol.v5.comm

import com.ibm.spark.kernel.protocol.v5.comm.CommCallbacks._
import com.ibm.spark.kernel.protocol.v5._
import org.scalatest.{Matchers, FunSpec}

class CommCallbacksSpec extends FunSpec with Matchers {

  private val testOpenCallback: OpenCallback = (UUID, String, Data) => {}
  private val testMsgCallback: MsgCallback = (UUID, Data) => {}
  private val testCloseCallback: CloseCallback = (UUID, Data) => {}

  private val failOpenCallback: OpenCallback =
    (UUID, String, Data) => throw new Throwable()
  private val failMsgCallback: MsgCallback =
    (UUID, Data) => throw new Throwable()
  private val failCloseCallback: CloseCallback =
    (UUID, Data) => throw new Throwable()

  describe("CommCallbacks") {
    describe("#addOpenCallback") {
      it("should append the provided callback to the internal list") {
        val commCallbacks = new CommCallbacks()
          .addOpenCallback(testOpenCallback)

        commCallbacks.openCallbacks should contain (testOpenCallback)
      }
    }

    describe("#addMsgCallback") {
      it("should append the provided callback to the internal list") {
        val commCallbacks = new CommCallbacks()
          .addMsgCallback(testMsgCallback)

        commCallbacks.msgCallbacks should contain (testMsgCallback)
      }
    }

    describe("#addCloseCallback") {
      it("should append the provided callback to the internal list") {
        val commCallbacks = new CommCallbacks()
          .addCloseCallback(testCloseCallback)

        commCallbacks.closeCallbacks should contain (testCloseCallback)
      }
    }

    describe("#executeOpenCallbacks") {
      it("should return an empty sequence of results if no callbacks exist") {
        new CommCallbacks().executeOpenCallbacks("", "", Data()) shouldBe empty
      }

      it("should return a sequence of try results if callbacks exist") {
        val commCallbacks = new CommCallbacks()
          .addOpenCallback(testOpenCallback)

        val results = commCallbacks.executeOpenCallbacks("", "", Data())

        results.head.isSuccess should be (true)
      }

      it("should return a sequence with failures if callbacks fail") {
        val commCallbacks = new CommCallbacks()
          .addOpenCallback(failOpenCallback)

        val results = commCallbacks.executeOpenCallbacks("", "", Data())

        results.head.isFailure should be (true)
      }
    }

    describe("#executeMsgCallbacks") {
      it("should return an empty sequence of results if no callbacks exist") {
        new CommCallbacks().executeMsgCallbacks("", Data()) shouldBe empty
      }

      it("should return a sequence of try results if callbacks exist") {
        val commCallbacks = new CommCallbacks()
          .addMsgCallback(testMsgCallback)

        val results = commCallbacks.executeMsgCallbacks("", Data())

        results.head.isSuccess should be (true)
      }

      it("should return a sequence with failures if callbacks fail") {
        val commCallbacks = new CommCallbacks()
          .addMsgCallback(failMsgCallback)

        val results = commCallbacks.executeMsgCallbacks("", Data())

        results.head.isFailure should be (true)
      }
    }

    describe("#executeCloseCallbacks") {
      it("should return an empty sequence of results if no callbacks exist") {
        new CommCallbacks().executeCloseCallbacks("", Data()) shouldBe empty
      }

      it("should return a sequence of try results if callbacks exist") {
        val commCallbacks = new CommCallbacks()
          .addCloseCallback(testCloseCallback)

        val results = commCallbacks.executeCloseCallbacks("", Data())

        results.head.isSuccess should be (true)
      }

      it("should return a sequence with failures if callbacks fail") {
        val commCallbacks = new CommCallbacks()
          .addCloseCallback(failCloseCallback)

        val results = commCallbacks.executeCloseCallbacks("", Data())

        results.head.isFailure should be (true)
      }
    }
  }
}