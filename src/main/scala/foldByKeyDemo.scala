import org.apache.spark.sql.SparkSession

/**
 * Created by shakeer on Jan, 2021
 **/
object foldByKeyDemo {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    val rdd =spark.sparkContext.makeRDD(Array(("A",10),("B",20),("C",30),("D",40),("A",50),("B",60),("C",70),("D",80)),2)
    val rdd2=rdd.foldByKey(2)((x,y)=>(x*y))
    rdd2.collect().foreach(println)
  }
}
