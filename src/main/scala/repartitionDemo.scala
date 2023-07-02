import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created by shakeer on Jan, 2021
 **/
object repartitionDemo {
  //repartition
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("repartition")
      .master("local")
      .getOrCreate()
    val rdd1=spark.sparkContext.makeRDD(Array(("A",1000),("B",2000),("C",3000),("D",5000),("A",5000),("C",10000),("B",200),("D",7999)))
    println(rdd1.partitions.size)
    val rdd2=rdd1.repartition(3)
    println(rdd2.partitions.size)
    //colaesce
    val rdd=spark.sparkContext.makeRDD(Array(("A",1000),("B",2000),("C",3000),("D",5000),("A",5000),("C",10000),("B",200),("D",7999)),4)
    println("Number of partitions",rdd.partitions.size)
    val rddclse=rdd.coalesce(1)
    println(rddclse.partitions.size)



    //
  }
}
