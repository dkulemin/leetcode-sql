package easy

import org.apache.spark.sql.{functions => F}
import utils.Utils.sparkReadPGTable

object BiggestSingleNumber extends App {
  /**
   * Table: MyNumbers
   *
   * +-------------+------+
   * | Column Name | Type |
   * +-------------+------+
   * | num         | int  |
   * +-------------+------+
   * This table may contain duplicates (In other words, there is no primary key for this table in SQL).
   * Each row of this table contains an integer.
   *
   *
   * A single number is a number that appeared only once in the MyNumbers table.
   *
   * Find the largest single number. If there is no single number, report null.
   *
   * https://leetcode.com/problems/biggest-single-number/description/
   * */

  sparkReadPGTable("BiggestSingleNumber")("mynumbers")
    .groupBy("num")
    .count()
    .filter(F.col("count") === 1)
    .agg(F.max(F.col("num")))
    .show()
}
