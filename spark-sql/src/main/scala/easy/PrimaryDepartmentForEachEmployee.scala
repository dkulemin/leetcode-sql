package easy

import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.expressions.Window
import utils.Utils.sparkReadPGTable

object PrimaryDepartmentForEachEmployee extends App {
  /**
   * Table: Employee
   *
   * +---------------+---------+
   * | Column Name   |  Type   |
   * +---------------+---------+
   * | employee_id   | int     |
   * | department_id | int     |
   * | primary_flag  | varchar |
   * +---------------+---------+
   * (employee_id, department_id) is the primary key (combination of columns with unique values) for this table.
   * employee_id is the id of the employee.
   * department_id is the id of the department to which the employee belongs.
   * primary_flag is an ENUM (category) of type ('Y', 'N'). If the flag is 'Y',
   * the department is the primary department for the employee. If the flag is 'N',
   * the department is not the primary.
   *
   * Employees can belong to multiple departments. When the employee joins other departments,
   * they need to decide which department is their primary department.
   * Note that when an employee belongs to only one department, their primary column is 'N'.
   *
   * Write a solution to report all the employees with their primary department.
   * For employees who belong to one department, report their only department.
   *
   * Return the result table in any order.
   *
   * https://leetcode.com/problems/primary-department-for-each-employee/description/
   * */

  sparkReadPGTable("PrimaryDepartmentForEachEmployee")("employee_1789")
    .withColumn("departments_cnt", F.count("department_id").over(Window.partitionBy("employee_id")))
    .filter(F.col("departments_cnt") === 1 || F.col("primary_flag") === "Y")
    .select("employee_id", "department_id")
    .show()
}
