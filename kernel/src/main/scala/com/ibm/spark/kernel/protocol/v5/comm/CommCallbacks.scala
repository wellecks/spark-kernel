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
package com.ibm.spark.kernel.protocol.v5.comm

import com.ibm.spark.kernel.protocol.v5._

import scala.util.Try

object CommCallbacks {
  type OpenCallback = (UUID, String, Data) => Unit
  type MsgCallback = (UUID, Data) => Unit
  type CloseCallback = (UUID, Data) => Unit
}

import CommCallbacks._

/**
 * Represents available callbacks to be triggered when various Comm events
 * are triggered.
 */

/**
 * Represents available callbacks to be triggered when various Comm events
 * are triggered.
 *
 * @param openCallbacks The sequence of open callbacks
 * @param msgCallbacks The sequence of msg callbacks
 * @param closeCallbacks The sequence of close callbacks
 */
class CommCallbacks(
  private[comm] val openCallbacks: Seq[CommCallbacks.OpenCallback] = Nil,
  private[comm] val msgCallbacks: Seq[CommCallbacks.MsgCallback] = Nil,
  private[comm] val closeCallbacks: Seq[CommCallbacks.CloseCallback] = Nil
) {

  /**
   * Adds a new open callback to be triggered.
   *
   * @param openCallback The open callback to add
   *
   * @return The updated CommCallbacks instance
   */
  def addOpenCallback(openCallback: OpenCallback): CommCallbacks =
    new CommCallbacks(
      openCallbacks :+ openCallback,
      msgCallbacks,
      closeCallbacks
    )

  /**
   * Adds a new msg callback to be triggered.
   *
   * @param msgCallback The msg callback to add
   *
   * @return The updated CommCallbacks instance
   */
  def addMsgCallback(msgCallback: MsgCallback): CommCallbacks =
    new CommCallbacks(
      openCallbacks,
      msgCallbacks :+ msgCallback,
      closeCallbacks
    )

  /**
   * Adds a new close callback to be triggered.
   *
   * @param closeCallback The close callback to add
   *
   * @return The updated CommCallbacks instance
   */
  def addCloseCallback(closeCallback: CloseCallback): CommCallbacks =
    new CommCallbacks(
      openCallbacks,
      msgCallbacks,
      closeCallbacks :+ closeCallback
    )

  /**
   * Executes all registered open callbacks and returns a sequence of results.
   *
   * @param commId The Comm Id to pass to all open callbacks
   * @param targetName The Comm Target Name to pass to all open callbacks
   * @param data The data to pass to all open callbacks
   *
   * @return The sequence of results from trying to execute callbacks
   */
  def executeOpenCallbacks(commId: UUID, targetName: String, data: Data) =
    openCallbacks.map(f => Try(f(commId, targetName, data)))

  /**
   * Executes all registered msg callbacks and returns a sequence of results.
   *
   * @param commId The Comm Id to pass to all msg callbacks
   * @param data The data to pass to all msg callbacks
   *
   * @return The sequence of results from trying to execute callbacks
   */
  def executeMsgCallbacks(commId: UUID, data: Data) =
    msgCallbacks.map(f => Try(f(commId, data)))

  /**
   * Executes all registered close callbacks and returns a sequence of results.
   *
   * @param commId The Comm Id to pass to all close callbacks
   * @param data The data to pass to all close callbacks
   *
   * @return The sequence of results from trying to execute callbacks
   */
  def executeCloseCallbacks(commId: UUID, data: Data) =
    closeCallbacks.map(f => Try(f(commId, data)))
}