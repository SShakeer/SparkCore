import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by shakeer on Jan, 2021
 **/
object pairRDDOperations {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("pair RDD").getOrCreate()
    val rdd=spark.sparkContext.makeRDD(Array(("A",1000),("B",2000),("C",3000),("D",5000),("A",5000),("C",10000),("B",200),("D",7999)))
   // rdd.collect.foreach(println)
   //finding max and min in pair RDD
   val grp=rdd.groupByKey()
    val max=grp.map(x=>(x._1,x._2.max))
    max.collect.foreach(println)
    println("printing minimum")
    val min=grp.map(x=>(x._1,x._2.min))
    min.collect.foreach(println)
//display in sorted
    println("*****************")
  min.sortBy(x=>x._1).collect.foreach(println)
    //sum of elements of pair RDD
    grp.map(x=>(x._1,x._2.sum)).collect.foreach(println)
//average
    println("average")
    grp.map(x=>(x._1,x._2.sum/x._2.size)).collect.foreach(println)

  }
}
