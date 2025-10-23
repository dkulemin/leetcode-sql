package easy

import org.apache.spark.sql.SparkSession

object ReplaceEmployeeIDWithTheUniqueIdentifier extends App {
  /**
   * Table: Employees
   *
   * +---------------+---------+
   * | Column Name   | Type    |
   * +---------------+---------+
   * | id            | int     |
   * | name          | varchar |
   * +---------------+---------+
   * id is the primary key (column with unique values) for this table.
   * Each row of this table contains the id and the name of an employee in a company.
   *
   * Table: EmployeeUNI
   *
   * +---------------+---------+
   * | Column Name   | Type    |
   * +---------------+---------+
   * | id            | int     |
   * | unique_id     | int     |
   * +---------------+---------+
   * (id, unique_id) is the primary key (combination of columns with unique values) for this table.
   * Each row of this table contains the id and the corresponding unique id of an employee in the company.
   *
   * Write a solution to show the unique ID of each user,
   * If a user does not have a unique ID replace just show null.
   *
   * Return the result table in any order.
   *
   * https://leetcode.com/problems/replace-employee-id-with-the-unique-identifier/description/
   * */

  val spark = SparkSession.builder()
    .appName("ReplaceEmployeeIDWithTheUniqueIdentifier")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars", "./jars/postgresql-42.7.8.jar")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val employeesDf = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/leetcodedb")
    .option("dbtable", "employees")
    .option("user", "leetcodeuser")
    .option("password", "pgpwd4leetcode")
    .option("driver", "org.postgresql.Driver")
    .load()

  val employeeUNIDf = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/leetcodedb")
    .option("dbtable", "employeeuni")
    .option("user", "leetcodeuser")
    .option("password", "pgpwd4leetcode")
    .option("driver", "org.postgresql.Driver")
    .load()

  employeesDf.join(employeeUNIDf, "id", "left").select("unique_id", "name").show()
}
