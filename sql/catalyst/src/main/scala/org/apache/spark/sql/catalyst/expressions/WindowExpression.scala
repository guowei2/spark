/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute


/**
 * @param child the computation being performed
 * @param windowSpec the window spec definition
 */
case class WindowExpression(child: Expression, windowSpec: WindowSpec) extends UnaryExpression {

  override type EvaluatedType = Any

  override def eval(input: Row) = child.eval(input)

  override def dataType = child.dataType
  override def nullable = child.nullable
  override def foldable = child.foldable

  override def toString: String = s"$child $windowSpec"
}

case class WindowSpec(windowPartition: WindowPartition, windowFrame: Option[WindowFrame])

case class WindowPartition(partitionBy: Seq[Expression], sortBy: Seq[SortOrder])

sealed trait FrameType

case object RowFrame extends FrameType
case object RangeFrame extends FrameType

case class WindowFrame(frameType: FrameType, preceding: Int, following: Int)
