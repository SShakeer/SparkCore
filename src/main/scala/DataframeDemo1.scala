import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

/**
 * Created by Basha on Feb, 2021
 **/
object DataframeDemo1 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.
  builder().master("local")
      .appName("df")
      .getOrCreate()
    import spark.implicits._
  val df=Seq(
    (10,"Accounting"),
    (20,"IT"),
    (30,"Hardware")
  ).toDF("deptno","Dname")

    df.printSchema()
    df.show()
//second method


    import spark.implicits._
    val df2=spark.sparkContext.parallelize(
      Seq(
      Row(10,"Accounting"),
      Row(20,"IT"),
      Row(30,"Hardware")))
    //row is used to access element by index number

    val schema11 = new StructType()
      .add(StructField("deptno", IntegerType, true))
      .add(StructField("dname", StringType, true))

   val df3=spark.createDataFrame(df2,schema11)
    df3.show()



    //reading dataset

    val df4=spark.read.format("csv").option("header","true")
      .option("inferSchema","true")
      .load("src/main/resources/department.csv")
      df4.printSchema()
  }
}
