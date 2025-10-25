package easy

import org.apache.spark.sql.{functions => F}
import utils.Utils.sparkReadPGTable

object ArticleViewsI extends App {
  /**
   * +---------------+---------+
   * | Column Name   | Type    |
   * +---------------+---------+
   * | article_id    | int     |
   * | author_id     | int     |
   * | viewer_id     | int     |
   * | view_date     | date    |
   * +---------------+---------+
   * There is no primary key (column with unique values) for this table, the table may have duplicate rows.
   * Each row of this table indicates that some viewer viewed an article (written by some author) on some date.
   * Note that equal author_id and viewer_id indicate the same person.
   *
   * Write a solution to find all the authors that viewed at least one of their own articles.
   *
   * Return the result table sorted by id in ascending order.
   *
   * https://leetcode.com/problems/article-views-i/description/
   * */

  val session = sparkReadPGTable("ArticleViewsI")
  val viewsDf = session("views")
  viewsDf.filter(F.col("author_id") === F.col("viewer_id"))
    .select(F.col("author_id").alias("id"))
    .sort()
    .distinct()
    .show()
}
