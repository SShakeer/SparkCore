import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by shakeer on Jan, 2021
 **/
object MapDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName("spark dept wise sum salary")
      .getOrCreate()

    import spark.implicits._

    val data =  spark.read.textFile("src/main/resources/emp.txt").rdd

    val res = data.map{ x =>
      val w = x.trim().split(",")
      val empno = w(0)
      val ename = w(1).toLowerCase
      val fc = ename.slice(0,1).toUpperCase
      val sal = w(2).toInt
      val grade = if (sal >= 5000) "A" else
      if (sal>=3000) "B" else
      if (sal>=2000) "C" else "D"
      val deptno = w(3).toInt
      val dname = deptno match {
        case 10 =>"MARKETING"
        case 20 => "SOFTWARE"
        case 30 => "SALES"
        case others =>"Others"
      }
      List(empno,ename,w(2),grade,dname).mkString("\t")

    }
    res.collect.foreach(println)
  }

}
