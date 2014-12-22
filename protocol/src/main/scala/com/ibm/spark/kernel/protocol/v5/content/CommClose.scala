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

package com.ibm.spark.kernel.protocol.v5.content

import com.ibm.spark.kernel.protocol.v5.{KernelMessageContent, Data, UUID}
import play.api.libs.json.Json

case class CommClose(comm_id: UUID, data: Data)
  extends KernelMessageContent with CommContent
{
  override def content : String =
    Json.toJson(this)(CommClose.commCloseWrites).toString
}

object CommClose {
  implicit val commCloseReads = Json.reads[CommClose]
  implicit val commCloseWrites = Json.writes[CommClose]
}
