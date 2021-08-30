import org.apache.spark.sql.SparkSession

object DropDatabase {
  def main(args: Array[String]): Unit = {
    var dbName = "dex_tpcds_sf1_withdecimal_withdate_withnulls"
    if (args(0) != null) {
      dbName = args(0)
    }

    val sc = SparkSession.builder
      .config("spark.sql.retainGroupColumns", value = false)
      .config("spark.sql.crossJoin.enabled", value = true)
      .getOrCreate


    println("-------SHOW DATABASES-----")
    sc.sql("SHOW DATABASES").show(numRows = 200, truncate = false)

    println("-------DROP DATABASES-----" + dbName)
    sc.sql("DROP DATABASE IF EXISTS " + dbName + " CASCADE").show(numRows = 200, truncate = false)

    println("-------SHOW DATABASES-----")
    sc.sql("SHOW DATABASES").show(numRows = 200, truncate = false)
  }
}
