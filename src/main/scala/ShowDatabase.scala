import org.apache.spark.sql.SparkSession

object ShowDatabase {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder
      .config("spark.sql.retainGroupColumns", value = false)
      .config("spark.sql.crossJoin.enabled", value = true)
      .getOrCreate

    var dbName = "dex_tpcds_sf1_withdecimal_withdate_withnulls"
    if (args(0) != null) {
      dbName = args(0)
    }


    println("-------SHOW DATABASES-----")
    sc.sql("SHOW DATABASES").show(numRows = 200, truncate = false)


    sc.sql("use " + dbName).show(numRows = 200, truncate = false)
    println("-------SHOW TABLES-----")
    sc.sql("SHOW TABLES").show(numRows = 200, truncate = false)

  }
}
