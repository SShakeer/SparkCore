import org.apache.spark.sql.SparkSession

/**
 * Created by shakeer on Mar, 2021
 **/
object readDB {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .getOrCreate()


    val empDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:scott/tiger@127.0.0.1:1521/ORCL")
      .option("dbtable", "scott.emp")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()

    empDF.write.format("csv").saveAsTable("db.sample")
  }
}
