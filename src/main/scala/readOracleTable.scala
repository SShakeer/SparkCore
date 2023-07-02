import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * Created by Basha on Mar, 2021
 **/
object readOracleTable {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local")
      .appName("reading oracle table")
      .getOrCreate()
      //ojdbc6.jar

    val urlvalue="jdbc:oracle:thin:scott/tiger@127.0.0.1:1521/ORCL"
    val driver1="oracle.jdbc.driver.OracleDriver"

    val emp=spark.read.format("jdbc")
      .option("url",urlvalue)
      .option("dbtable","scott.emp")
      .option("user","scott")
      .option("password","tiger")
      .option("driver",driver1)
      .load()
    //emp.printSchema()

    val dept=spark.read.format("jdbc")
      .option("url",urlvalue)
      .option("dbtable","scott.dept")
      .option("user","scott")
      .option("password","tiger")
      .option("driver",driver1)
      .load()
//dept.printSchema()
    //1.SQL approache
    val tabledf=emp.createOrReplaceTempView("emp_temp")
    //val readcols=spark.sql(s"""select deptno,ename,sal from emp_temp""").show()
    //2. Dataframe API
    //Empno,Ename,Dname,Deptno,Sal
    val joindf=emp.join(dept,Seq("Deptno"),"right_outer").select("Empno","Ename","Dname","Deptno","sal")
    //val dffilter=joindf.filter(col("Deptno")===10).show()

    val df2=emp.join(dept,emp("deptno")===dept("deptno"),"inner")


//df2.persist(StorageLevel.MEMORY_AND_DISK_SER_2)


    Thread.sleep(100000)



  }
}
