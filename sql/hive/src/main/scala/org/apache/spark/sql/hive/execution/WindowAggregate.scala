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

package org.apache.spark.sql.hive.execution

import java.util.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.{UnaryNode, SparkPlan, Sort}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.hive.HiveGenericUdaf
import org.apache.spark.rdd.RDD


/**
 * :: DeveloperApi ::
 * Groups input data by `partitionExpressions` and computes the `windowExpressions` for each
 * group.
 * @param partitionExpressions expressions that are evaluated to determine partition.
 * @param windowExpressions computeExpressions that compute now for each partition.
 * @param otherExpressions otherExpressions that are expressions except computeExpressions.
 * @param child the input data source.
 */
@DeveloperApi
case class WindowAggregate(
    partitionExpressions: Seq[Expression],
    windowExpressions: Seq[Alias],
    otherExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode {

  override def requiredChildDistribution: List[Distribution]  =
    if (partitionExpressions == Nil) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(partitionExpressions) :: Nil
    }

  // HACK: Generators don't correctly preserve their output through serializations so we grab
  // out child's output attributes statically here.
  private[this] val childOutput = child.output

  override def output: Seq[Attribute] = (windowExpressions ++ otherExpressions).map(_.toAttribute)

  case class ComputedWindow(
      unboundFunction: Expression,
      pivotResult: Boolean,
      windowSpec: WindowSpec,
      boundedFunction: Expression,
      computedAttribute: AttributeReference)

  case class WindowFunctionInfo(
      supportsWindow: Boolean, pivotResult: Boolean, impliesOrder: Boolean)

  private[this] val computedWindows = windowExpressions.collect{

    case Alias(expr @ WindowExpression(func, spec), _) =>
      val ipr = func match {
        case HiveGenericUdaf(_, wfi, _) => wfi.isPivotResult
        case _ => false
      }
      ComputedWindow(
        func,
        ipr,
        spec,
        BindReferences.bindReference(func, child.output),
        AttributeReference(s"funcResult:$func", func.dataType, func.nullable)())
  }

  private[this] val otherAttributes = otherExpressions.map(_.toAttribute)

  /** The schema of the result of all evaluations */
  private[this] val resultAttributes = otherAttributes ++ computedWindows.map(_.computedAttribute)

  private[this] val resultMap =
    (otherExpressions.map { other => other -> other.toAttribute } ++
      computedWindows.map { window => window.unboundFunction -> window.computedAttribute }).toMap

  private[this] val resultExpressions = (windowExpressions ++ otherExpressions).map { sel =>
    sel.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  }

  private[this] val sortExpressions = child match {
    case Sort(sortOrder, _, _) => sortOrder
    case _ => Seq[SortOrder]()
  }

  // check whether to sort by other key in one partition
  private[this] val ifSortInOnePartition =
    !sortExpressions.isEmpty &&
      !sortExpressions.map(_.child).diff(partitionExpressions).isEmpty

  private[this] val sortReference =
    if (sortExpressions.isEmpty) None
    else {
      // this is for computing with range frame ,and it only support 1 order
      Some(BindReferences.bindReference(sortExpressions.head, childOutput))
    }

  private[this] def computeFunctions(rows: CompactBuffer[Row]): Seq[Iterator[Any]] =
    computedWindows.map{ window =>
      val baseExpr = window.boundedFunction.asInstanceOf[AggregateExpression]
      window.windowSpec.windowFrame.map { frame =>
        frame.frameType match {
          case RowFrame => rowFrameFunction(baseExpr, frame, rows).iterator
          case RangeFrame => rangeFrameFunction(baseExpr, frame, rows).iterator
        }
      }.getOrElse {
        val function = baseExpr.newInstance()
        if (window.pivotResult) {
          rows.foreach(function.update)
          function.eval(EmptyRow).asInstanceOf[Seq[Any]].iterator
        } else if (ifSortInOnePartition) {
          rows.map { row =>
            function.update(row)
            function.eval(EmptyRow)
          }.iterator
        } else {
          rows.foreach(function.update)
          val result = function.eval(EmptyRow)
          (0 to rows.size - 1).map(r => result).iterator
        }

      }
    }

  private[this] def rowFrameFunction(base: AggregateExpression, frame: WindowFrame,
      rows: CompactBuffer[Row]): CompactBuffer[Any] = {

    val frameResults = new CompactBuffer[Any]()
    var rowIndex = 0
    while (rowIndex < rows.size) {
      var start =
        if (frame.preceding == Int.MaxValue) 0
        else rowIndex - frame.preceding
      if (start < 0) start = 0
      var end =
        if (frame.following == Int.MaxValue) {
          rows.size - 1
        } else {
          rowIndex + frame.following
        }
      if (end > rows.size - 1) end = rows.size - 1

      // new aggregate function
      val aggr = base.newInstance()
      (start to end).foreach(i => aggr.update(rows(i)))

      frameResults += aggr.eval(EmptyRow)
      rowIndex += 1
    }
    frameResults
  }

  private[this] def rangeFrameFunction(base: AggregateExpression, frame: WindowFrame,
      rows: CompactBuffer[Row]): CompactBuffer[Any] = {

    val (preceding, following) = sortReference.map { sortExpression =>
      sortExpression.child.dataType match {
        case IntegerType => (Literal(frame.preceding), Literal(frame.following))
        case LongType => (Literal(frame.preceding.toLong), Literal(frame.following.toLong))
        case DoubleType => (Literal(frame.preceding.toDouble), Literal(frame.following.toDouble))
        case FloatType => (Literal(frame.preceding.toFloat), Literal(frame.following.toFloat))
        case ShortType => (Literal(frame.preceding.toShort), Literal(frame.following.toShort))
        case DecimalType() =>
          (Literal(BigDecimal(frame.preceding)), Literal(BigDecimal(frame.following)))
        // TODO: need to support StringType comparison
        case StringType => throw new Exception(s"not support StringType comparison yet")
        case dt => throw new Exception(s"not support $dt comparison")
      }
    }.getOrElse {
      throw new Exception(s"not support range frame with no sort expression ")
    }

    val frameResults = new CompactBuffer[Any]()
    var rowIndex = 0
    while (rowIndex < rows.size) {

      var precedingIndex = 0
      var followingIndex = rows.size - 1

      sortReference.map { sortExpression =>
        val currentRow = rows(rowIndex)
        val eval = sortExpression.child.eval(currentRow)
        val precedingExpr =
          if (sortExpression.direction == Ascending) {
            Literal(eval) - sortExpression.child <= preceding
          } else {
            sortExpression.child - Literal(eval) <= preceding
          }

        val followingExpr =
          if (sortExpression.direction == Ascending) {
            sortExpression.child - Literal(eval) <= following
          } else {
            Literal(eval) - sortExpression.child <= following
          }

        if (frame.preceding != Int.MaxValue) precedingIndex = rowIndex
        while (precedingIndex > 0 &&
          precedingExpr.eval(rows(precedingIndex - 1)).asInstanceOf[Boolean]) {
          precedingIndex -= 1
        }

        if (frame.following != Int.MaxValue) followingIndex = rowIndex
        while (followingIndex < rows.size - 1 &&
          followingExpr.eval(rows(followingIndex + 1)).asInstanceOf[Boolean]) {
          followingIndex += 1
        }
      }
      // new aggregate function
      val aggr = base.newInstance()
      (precedingIndex to followingIndex).foreach(i => aggr.update(rows(i)))
      frameResults += aggr.eval(EmptyRow)
      rowIndex += 1
    }
    frameResults
  }

  private[this] def getNextFunctionsRow(
      functionsResult: Seq[Iterator[Any]]): GenericMutableRow = {
    val result = new GenericMutableRow(functionsResult.length)
    var i = 0
    while (i < functionsResult.length) {
      result(i) = functionsResult(i).next
      i += 1
    }
    result
  }


  override def execute(): RDD[Row] = attachTree(this, "execute") {
    if (partitionExpressions.isEmpty) {
      child.execute().mapPartitions { iter =>

        val resultProjection = new InterpretedProjection(resultExpressions, resultAttributes)

        val otherProjection = new InterpretedMutableProjection(otherAttributes, childOutput)
        val joinedRow = new JoinedRow

        val rows = new CompactBuffer[Row]()
        while (iter.hasNext) {
          rows += iter.next().copy()
        }
        new Iterator[Row] {
          private[this] val functionsResult = computeFunctions(rows)
          private[this] var currentRowIndex: Int = 0

          override final def hasNext: Boolean = currentRowIndex < rows.size

          override final def next(): Row = {

            val otherResults = otherProjection(rows(currentRowIndex)).copy()
            currentRowIndex += 1
            resultProjection(joinedRow(otherResults,getNextFunctionsRow(functionsResult)))
          }
        }

      }
    } else {
      child.execute().mapPartitions { iter =>
        val partitionTable = new HashMap[Row, CompactBuffer[Row]]
        val partitionProjection =
          new InterpretedMutableProjection(partitionExpressions, childOutput)

        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          val partitionKey = partitionProjection(currentRow).copy()
          val existingMatchList = partitionTable.get(partitionKey)
          val matchList = if (existingMatchList == null) {
            val newMatchList = new CompactBuffer[Row]()
            partitionTable.put(partitionKey, newMatchList)
            newMatchList
          } else {
            existingMatchList
          }
          matchList += currentRow.copy()
        }

        new Iterator[Row] {
          private[this] val partitionTableIter = partitionTable.entrySet().iterator()
          private[this] var currentpartition: CompactBuffer[Row] = _
          private[this] var functionsResult: Seq[Iterator[Any]] = _
          private[this] var currentRowIndex: Int = -1

          val resultProjection = new InterpretedProjection(resultExpressions, resultAttributes)
          val otherProjection = new InterpretedMutableProjection(otherAttributes, childOutput)
          val joinedRow = new JoinedRow

          override final def hasNext: Boolean =
            (currentRowIndex != -1 && currentRowIndex < currentpartition.size) ||
              (partitionTableIter.hasNext && fetchNext())

          override final def next(): Row = {

            val otherResults = otherProjection(currentpartition(currentRowIndex)).copy()
            currentRowIndex += 1
            resultProjection(joinedRow(otherResults,getNextFunctionsRow(functionsResult)))

          }

          private final def fetchNext(): Boolean = {

            currentRowIndex = 0
            if (partitionTableIter.hasNext) {
              currentpartition = partitionTableIter.next().getValue
              functionsResult = computeFunctions(currentpartition)
              true
            } else false
          }
        }

      }
    }
  }
}
