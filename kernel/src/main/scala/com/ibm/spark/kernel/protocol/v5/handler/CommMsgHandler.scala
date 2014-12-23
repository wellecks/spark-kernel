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

package com.ibm.spark.kernel.protocol.v5.handler

import com.ibm.spark.kernel.protocol.v5.comm.{CommWriter, CommStorage}
import com.ibm.spark.kernel.protocol.v5.content.CommMsg
import com.ibm.spark.kernel.protocol.v5.{KMBuilder, Utilities, ActorLoader, KernelMessage}
import com.ibm.spark.utils.MessageLogSupport
import play.api.data.validation.ValidationError
import play.api.libs.json.JsPath

import scala.concurrent.Future
import scala.concurrent.future
import scala.concurrent.ExecutionContext.Implicits.global

class CommMsgHandler(actorLoader: ActorLoader, commStorage: CommStorage)
  extends BaseHandler(actorLoader) with MessageLogSupport
{
  override def process(kernelMessage: KernelMessage): Future[_] = {
    logKernelMessageAction("Initiating Comm Msg for", kernelMessage)
    Utilities.parseAndHandle(
      kernelMessage.contentString,
      CommMsg.commMsgReads,
      handler = handleCommMsg,
      errHandler = handleParseError
    )
  }

  private def handleCommMsg(commMsg: CommMsg) = future {
    val commId = commMsg.comm_id
    val data = commMsg.data

    logger.debug(s"Received comm_msg with id '$commId'")

    // TODO: Should we be reusing something from the KernelMessage?
    val commWriter = new CommWriter(actorLoader, KMBuilder(), commId)

    if (commStorage.contains(commId)) {
      logger.debug(s"Executing msg callbacks for id '$commId'")

      // TODO: Should we be checking the return values? Probably not.
      commStorage(commId).executeMsgCallbacks(commWriter, commId, data)
    } else {
      logger.warn(s"Received invalid id for Comm Msg: $commId")
    }
  }

  private def handleParseError(invalid: Seq[(JsPath, Seq[ValidationError])]) =
    future {
      // TODO: Determine proper response for a parse failure
      logger.warn("Parse error for Comm Msg! Not responding!")
    }

}

