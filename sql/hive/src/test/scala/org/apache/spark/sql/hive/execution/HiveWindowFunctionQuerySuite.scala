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

import java.util.{Locale, TimeZone}

import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.util.Utils
import org.scalatest.BeforeAndAfter

/**
 * The test suite for window functions. To actually compare results with Hive,
 * every test should be created by `createQueryTest`. Because we are reusing tables
 * for different tests and there are a few properties needed to let Hive generate golden
 * files, every `createQueryTest` calls should explicitly set `reset` to `false`.
 */
class HiveWindowFunctionQuerySuite extends HiveComparisonTest with BeforeAndAfter {
  private val originalTimeZone = TimeZone.getDefault
  private val originalLocale = Locale.getDefault
  private val testTempDir = Utils.createTempDir()
  import org.apache.spark.sql.hive.test.TestHive.implicits._

  override def beforeAll() {
    TestHive.cacheTables = true
    // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
    // Add Locale setting
    Locale.setDefault(Locale.US)

    // Create the table used in windowing.q
    sql("DROP TABLE IF EXISTS part")
    sql(
      """
        |CREATE TABLE part(
        |  p_partkey INT,
        |  p_name STRING,
        |  p_mfgr STRING,
        |  p_brand STRING,
        |  p_type STRING,
        |  p_size INT,
        |  p_container STRING,
        |  p_retailprice DOUBLE,
        |  p_comment STRING)
      """.stripMargin)
    val testData = TestHive.getHiveFile("data/files/part_tiny.txt").getCanonicalPath
    sql(
      s"""
        |LOAD DATA LOCAL INPATH '$testData' overwrite into table part
      """.stripMargin)
    // The following settings are used for generating golden files with Hive.
    // We have to use kryo to correctly let Hive serialize plans with window functions.
    // This is used to generate golden files.
    sql("set hive.plan.serialization.format=kryo")
    // Explicitly set fs to local fs.
    sql(s"set fs.default.name=file://$testTempDir/")
    //sql(s"set mapred.working.dir=${testTempDir}")
    // Ask Hive to run jobs in-process as a single map and reduce task.
    sql("set mapred.job.tracker=local")
  }

  override def afterAll() {
    TestHive.cacheTables = false
    TimeZone.setDefault(originalTimeZone)
    Locale.setDefault(originalLocale)
    TestHive.reset()
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests from windowing.q
  /////////////////////////////////////////////////////////////////////////////
  createQueryTest("windowing.q -- 1. testWindowing",
    s"""
      |select p_mfgr, p_name, p_size,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |sum(p_retailprice) over
      |(distribute by p_mfgr sort by p_name rows between unbounded preceding and current row) as s1
      |from part
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 2. testGroupByWithPartitioning",
    s"""
      |select p_mfgr, p_name, p_size,
      |min(p_retailprice),
      |rank() over(distribute by p_mfgr sort by p_name)as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |p_size, p_size - lag(p_size,1,p_size) over(distribute by p_mfgr sort by p_name) as deltaSz
      |from part
      |group by p_mfgr, p_name, p_size
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 19. testUDAFsWithGBY",
    """
      |
      |select  p_mfgr,p_name, p_size, p_retailprice,
      |sum(p_retailprice) over w1 as s,
      |min(p_retailprice) as mi ,
      |max(p_retailprice) as ma ,
      |avg(p_retailprice) over w1 as ag
      |from part
      |group by p_mfgr,p_name, p_size, p_retailprice
      |window w1 as (distribute by p_mfgr sort by p_mfgr, p_name rows between 2 preceding and 2 following);
      |
    """.stripMargin, reset = false)

  createQueryTest("windowing.q -- 26. testGroupByHavingWithSWQAndAlias",
    """
      |select p_mfgr, p_name, p_size, min(p_retailprice) as mi,
      |rank() over(distribute by p_mfgr sort by p_name) as r,
      |dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
      |p_size, p_size - lag(p_size,1,p_size) over(distribute by p_mfgr sort by p_name) as deltaSz
      |from part
      |group by p_mfgr, p_name, p_size
      |having p_size > 0
    """.stripMargin, reset = false)
}
