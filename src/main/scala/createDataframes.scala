import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
//ACID

/**
 * Created by Basha on Mar, 2021
 **/
object createDataframes {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local")
      .appName("creating dataframes").getOrCreate()
    //1.toDF method
    import spark.implicits._
    val data=Seq((10,"Accounting"),(20,"IT"),(30,"HardWare")).toDF("Deptno","Dname")
    data.printSchema()
    data.show()
    //2. using createDataframe method
    val data2=spark.sparkContext.parallelize(Seq(
      Row(100,"MARK"),
      Row(200,"KING"),
      Row(300,"VENKAT")
    ))
    val empSchema=new StructType()
      .add(StructField("Empno",IntegerType,true))
      .add(StructField("Ename",StringType,true))
    val dataframe4=spark.createDataFrame(data2,empSchema)
    dataframe4.show()
 //3. reading external data
    val readcsv=spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("src/main/resources/department.csv")
    readcsv.show()
  }
}
