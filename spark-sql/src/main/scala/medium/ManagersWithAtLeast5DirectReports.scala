package medium

import org.apache.spark.sql.{functions => F}
import utils.Utils.sparkReadPGTable

object ManagersWithAtLeast5DirectReports extends App {
  /**
   * Table: Employee
   *
   * +-------------+---------+
   * | Column Name | Type    |
   * +-------------+---------+
   * | id          | int     |
   * | name        | varchar |
   * | department  | varchar |
   * | managerId   | int     |
   * +-------------+---------+
   * id is the primary key (column with unique values) for this table.
   * Each row of this table indicates the name of an employee, their department, and the id of their manager.
   * If managerId is null, then the employee does not have a manager.
   * No employee will be the manager of themself.
   *
   *
   * Write a solution to find managers with at least five direct reports.
   *
   * Return the result table in any order.
   *
   * https://leetcode.com/problems/managers-with-at-least-5-direct-reports/description/
   * */

  val session = sparkReadPGTable("ManagersWithAtLeast5DirectReports")
  val employeeDf = session("employee")

  employeeDf.alias("e1")
    .join(
      employeeDf.alias("e2"),
      F.col("e1.id") === F.col("e2.managerid")
    )
    .groupBy("e1.id", "e1.name")
    .agg(F.count("e1.id").alias("reportsCount"))
    .filter(F.col("reportsCount") > 4)
    .select("e1.name")
    .show()
}
