package easy

import org.apache.spark.sql.{functions => F}
import utils.Utils.sparkReadPGTable

object NumberOfEmployeesWhichReportToEachEmployee extends App {
  /**
   * Table: Employees
   *
   * +-------------+----------+
   * | Column Name | Type     |
   * +-------------+----------+
   * | employee_id | int      |
   * | name        | varchar  |
   * | reports_to  | int      |
   * | age         | int      |
   * +-------------+----------+
   * employee_id is the column with unique values for this table.
   * This table contains information about the employees and the id of the manager they report to.
   * Some employees do not report to anyone (reports_to is null).
   *
   * For this problem, we will consider a manager an employee who has at least 1 other employee reporting to them.
   *
   * Write a solution to report the ids and the names of all managers,
   * the number of employees who report directly to them,
   * and the average age of the reports rounded to the nearest integer.
   *
   * Return the result table ordered by employee_id.
   *
   * https://leetcode.com/problems/the-number-of-employees-which-report-to-each-employee/description/
   * */

  val employeeDF = sparkReadPGTable("NumberOfEmployeesWhichReportToEachEmployee")("employees_1731")

  employeeDF.alias("employee")
    .join(employeeDF.alias("manager"), F.col("employee.reports_to") === F.col("manager.employee_id"))
    .groupBy("manager.employee_id", "manager.name")
    .agg(
      F.count("employee.reports_to").alias("reports_count"),
      F.round(F.avg("employee.age").alias("average_age"))
    )
    .show()
}
