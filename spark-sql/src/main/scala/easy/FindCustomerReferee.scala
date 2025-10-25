package easy

import org.apache.spark.sql.{functions => F}
import utils.Utils.sparkReadPGTable

object FindCustomerReferee extends App {
  /**
   * +-------------+---------+
   * | Column Name | Type    |
   * +-------------+---------+
   * | id          | int     |
   * | name        | varchar |
   * | referee_id  | int     |
   * +-------------+---------+
   * In SQL, id is the primary key column for this table.
   * Each row of this table indicates the id of a customer, their name, and the id of the customer who referred them.
   *
   * Find the names of the customer that are either:
   * 1. referred by any customer with id != 2.
   * 2. not referred by any customer.
   *
   * Return the result table in any order.
   *
   * https://leetcode.com/problems/find-customer-referee/description/
   */

  val session = sparkReadPGTable("FindCustomerReferee")
  val customerDf = session("customer")
  customerDf.filter(
      !(F.col("referee_id") === 2) ||
        F.col("referee_id").isNull
    )
    .select("name")
    .show()
}
