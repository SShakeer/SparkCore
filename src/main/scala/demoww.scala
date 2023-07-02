import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created by shakeer on Jan, 2021
 **/

object demoww {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("word count example")
      .getOrCreate()


    val orders =  sparkSession.read.textFile("src/main/resources/wordcount.txt").rdd
    val df_flat=orders.flatMap(x=>x.split(" "))
    val df_map=df_flat.map(word=>(word,1))
    val s=df_map.reduceByKey(_ + _).foreach(println)
  }
}
