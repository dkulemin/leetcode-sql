package medium

import org.apache.spark.sql.{SparkSession, functions}

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

  val spark = SparkSession.builder()
    .appName("ManagersWithAtLeast5DirectReports")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars", "./jars/postgresql-42.7.8.jar")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val employeeDf = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/leetcodedb")
    .option("dbtable", "employee")
    .option("user", "leetcodeuser")
    .option("password", "pgpwd4leetcode")
    .option("driver", "org.postgresql.Driver")
    .load()

//  employeeDf.show()
  employeeDf.alias("e1")
    .join(
      employeeDf.alias("e2"),
      functions.col("e1.id") === functions.col("e2.managerid")
    )
    .groupBy("e1.id", "e1.name")
    .agg(functions.count("e1.id").alias("reportsCount"))
    .filter(functions.col("reportsCount") > 4)
    .select("e1.name")
    .show()
}
