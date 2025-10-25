package utils

import org.apache.spark.sql.{SparkSession, DataFrame}

object Utils {
  def returnSparkSession(appName: String): SparkSession = SparkSession.builder()
    .appName(appName)
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars", "./jars/postgresql-42.7.8.jar")
    .getOrCreate()

  def pgTableToDF(session: SparkSession, table: String, db: String = "leetcodedb"): DataFrame = session.read
    .format("jdbc")
    .option("url", s"jdbc:postgresql://127.0.0.1:5432/$db")
    .option("dbtable", table)
    .option("user", "leetcodeuser")
    .option("password", "pgpwd4leetcode")
    .option("driver", "org.postgresql.Driver")
    .load()

  def sparkReadPGTable(appName: String): String => DataFrame = (table: String) => {
    val session = returnSparkSession(appName)
    session.sparkContext.setLogLevel("WARN")
    pgTableToDF(session, table)
  }
}
