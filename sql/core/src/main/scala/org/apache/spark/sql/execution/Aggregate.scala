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

package org.apache.spark.sql.execution

import java.util.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.collection.{CompactBuffer, ExternalAppendOnlyMap}

/**
 * Groups input data by `groupingExpressions` and computes the `aggregateExpressions` for each
 * group.
 *
 *  - If true then aggregation is done partially on local data without shuffling to
 *    ensure all values where `groupingExpressions` are equal are present.
 *  - Expressions that are evaluated to determine grouping.
 *  - Expressions that are computed for each group.
 *  - The input data source.
 */
trait Aggregate{

  self: SparkPlan =>

  /** If true then aggregation is done partially on local data without shuffling to */
  val partial: Boolean
  /** Expressions that are evaluated to determine grouping */
  val groupingExpressions: Seq[Expression]
  /** Expressions that are computed for each group */
  val aggregateExpressions: Seq[NamedExpression]
  /** The input data source */
  val child: SparkPlan

  override def requiredChildDistribution =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  // HACK: Generators don't correctly preserve their output through serializations so we grab
  // out child's output attributes statically here.
  protected[this] val childOutput = child.output

  override def output = aggregateExpressions.map(_.toAttribute)

  /**
   * An aggregate that needs to be computed for each row in a group.
   *
   * @param unbound Unbound version of this aggregate, used for result substitution.
   * @param aggregate A bound copy of this aggregate used to create a new aggregation buffer.
   * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
   *                        output.
   */
  case class ComputedAggregate(
      unbound: AggregateExpression,
      aggregate: AggregateExpression,
      resultAttribute: AttributeReference)

  /** A list of aggregates that need to be computed for each group. */
  protected[this] val computedAggregates = aggregateExpressions.flatMap { agg =>
    agg.collect {
      case a: AggregateExpression =>
        ComputedAggregate(
          a,
          BindReferences.bindReference(a, childOutput),
          AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
    }
  }.toArray

  /** The schema of the result of all aggregate evaluations */
  protected[this] val computedSchema = computedAggregates.map(_.resultAttribute)

  /** Creates a new aggregate buffer for a group. */
  protected[this] def newAggregateBuffer(): CompactBuffer[AggregateFunction] = {
    val buffer = new CompactBuffer[AggregateFunction]
    var i = 0
    while (i < computedAggregates.length) {
      buffer += computedAggregates(i).aggregate.newInstance()
      i += 1
    }
    buffer
  }

  /** Named attributes used to substitute grouping attributes into the final result. */
  protected[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
   * A map of substitutions that are used to insert the aggregate expressions and grouping
   * expression into the final result expression.
   */
  private[this] val resultMap =
    (computedAggregates.map { agg => agg.unbound -> agg.resultAttribute } ++ namedGroups).toMap

  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */
  protected[this] val resultExpressions = aggregateExpressions.map { agg =>
    agg.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  }

  protected[this] def aggregateNoGrouping() = {
    child.execute().mapPartitions { iter =>
      val buffer = newAggregateBuffer()
      var currentRow: Row = null
      while (iter.hasNext) {
        currentRow = iter.next()
        var i = 0
        while (i < buffer.length) {
          buffer(i).update(currentRow)
          i += 1
        }
      }
      val resultProjection = new InterpretedProjection(resultExpressions, computedSchema)
      val aggregateResults = new GenericMutableRow(computedAggregates.length)

      var i = 0
      while (i < buffer.length) {
        aggregateResults(i) = buffer(i).eval(EmptyRow)
        i += 1
      }

      Iterator(resultProjection(aggregateResults))
    }
  }
}

case class OnHeapAggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with Aggregate{

  override def execute() = attachTree(this, "execute") {
    if (groupingExpressions.isEmpty) {
      aggregateNoGrouping()
    } else {
      child.execute().mapPartitions { iter =>
        val hashTable = new HashMap[Row, CompactBuffer[AggregateFunction]]
        val groupingProjection =
          new InterpretedMutableProjection(groupingExpressions, childOutput)

        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }

          var i = 0
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }

        new Iterator[Row] {
          private[this] val hashTableIter = hashTable.entrySet().iterator()
          private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
          private[this] val resultProjection =
            new InterpretedMutableProjection(
              resultExpressions, computedSchema ++ namedGroups.map(_._2))
          private[this] val joinedRow = new JoinedRow

          override final def hasNext: Boolean = hashTableIter.hasNext

          override final def next(): Row = {
            val currentEntry = hashTableIter.next()
            val currentGroup = currentEntry.getKey
            val currentBuffer = currentEntry.getValue

            var i = 0
            while (i < currentBuffer.length) {
              // Evaluating an aggregate buffer returns the result.  No row is required since we
              // already added all rows in the group using update.
              aggregateResults(i) = currentBuffer(i).eval(EmptyRow)
              i += 1
            }
            resultProjection(joinedRow(aggregateResults, currentGroup))
          }
        }
      }
    }
  }

}

case class ExternalAggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with Aggregate{

  override def execute() = attachTree(this, "execute") {
    if (groupingExpressions.isEmpty) {
      aggregateNoGrouping()
    } else {
      child.execute().mapPartitions { iter =>
        val groupingProjection =
          new InterpretedMutableProjection(groupingExpressions, childOutput)

        val createCombiner = (v: Row) =>{
          val c = newAggregateBuffer()
          var i = 0
          while (i < c.length) {
            c(i).update(v)
            i += 1
          }
          c
        }
        val mergeValue = (c: CompactBuffer[AggregateFunction], v: Row) => {
          var i = 0
          while (i < c.length) {
            c(i).update(v)
            i += 1
          }
          c
        }
        val mergeCombiners = (c1: CompactBuffer[AggregateFunction], c2: CompactBuffer[AggregateFunction]) => {
          var i = 0
          while (i < c1.length) {
            c1(i).merge(c2(i))
            i += 1
          }
          c1
        }
        val combiners = new ExternalAppendOnlyMap[Row, Row, CompactBuffer[AggregateFunction]](
          createCombiner, mergeValue, mergeCombiners)
        while (iter.hasNext) {
          val row = iter.next()
          combiners.insert(groupingProjection(row).copy(), row)
        }
        new Iterator[Row] {
          private[this] val externalIter = combiners.iterator
          private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
          private[this] val resultProjection =
            new InterpretedMutableProjection(
              resultExpressions, computedSchema ++ namedGroups.map(_._2))
          private[this] val joinedRow = new JoinedRow

          override final def hasNext: Boolean = externalIter.hasNext
          override final def next(): Row = {
            val currentEntry = externalIter.next()
            val currentGroup = currentEntry._1
            val currentBuffer = currentEntry._2

            var i = 0
            while (i < currentBuffer.length) {
              // Evaluating an aggregate buffer returns the result.  No row is required since we
              // already added all rows in the group using update.
              aggregateResults(i) = currentBuffer(i).eval(EmptyRow)
              i += 1
            }
            resultProjection(joinedRow(aggregateResults, currentGroup))
          }
        }
      }
    }
  }
}
