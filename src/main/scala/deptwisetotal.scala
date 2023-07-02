import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
/**
 * Created by Basha on Mar, 2021
 **/
object deptwisetotal {
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("dept wise sum slary")
      .getOrCreate()

    val urlvalue = "jdbc:oracle:thin:scott/tiger@127.0.0.1:1521/ORCL"
    val driver1 = "oracle.jdbc.driver.OracleDriver"

    val emp = spark.read.format("jdbc")
      .option("url", urlvalue)
      .option("dbtable", "scott.emp")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", driver1)
      .load().show()

    //val df=emp.groupBy(col("deptno")).agg(sum(col("sal")).alias("DeptwiseTotal"))

 // df.write.mode("append").format("jdbc").partitionBy("deptno").saveAsTable("")

    //emp.persist(StorageLevel.)

  }
}
