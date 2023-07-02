/**
 * Created by shakeer on Oct, 2020
 **/

import org.apache.spark.sql.SparkSession
object HelloRDD {
  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder()
      .master("local")
      .appName("Hello")
      .getOrCreate()

    val rdd=spark.read.textFile("src/main/resources/wordcount.txt").rdd
    val rdd1=rdd.map(x=>x.split(""))
    val rdd2=rdd1.flatMap(x=>x)
    rdd2.collect()

    val rdd3=rdd2.map(x=>(x,1))
    val rdd4=rdd3.reduceByKey((x,y)=>(x+y))
    rdd4.collect
    Thread.sleep(1000)
}
}
