import org.apache.spark.sql.SparkSession

import scala.io.Source

object SimpleJob {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("dude, i need at least one parameter")
      return
    }

    val range = args(0).split("-")
    if (range.length != 2) {
      println("dude, this need range")
      return
    }

    var rep = 1;
    if (args(1) != null && (new Integer(args(1))) > 0) {
      rep = new Integer(args(1))
    }

    var dbName = "dex_tpcds_sf1_withdecimal_withdate_withnulls"
    if (args(2) != null) {
      dbName = args(2)
    }

    val sc = SparkSession.builder
      .config("spark.sql.retainGroupColumns", value = false)
      .config("spark.sql.crossJoin.enabled", value = true)
      .getOrCreate

    sc.sql("SHOW databases").show(numRows = 200, truncate = false)
    // sc.sql("DROP TABLE dex_tpcds_sf1000_withdecimal_withdate_withnulls PURGE").show(truncate = false)
    sc.sql("use " + dbName).show
    sc.sql("SHOW tables").show

    for (j <- 1 to rep) {
      println("-------Running for-----" + j)
      for (i <- range(0).toInt to range(1).toInt) {
        println("-------Running SQL-----" + i)
        val ipFileStream = getClass.getResourceAsStream("queries/query" +
          (if (i < 10) "0" + i else i) +
          ".sql")
        val readLines = Source.fromInputStream(ipFileStream).getLines
        var sql = ""
        readLines.foreach(readLines => {
          sql += readLines + " "
        })
        ipFileStream.close()
        println(sql.replaceAll(" +", " ").trim)
        //      println(Queries.arr(i).replaceAll(" +", " ").trim)
        sc.sql(sql).show

      }
    }

  }

}
