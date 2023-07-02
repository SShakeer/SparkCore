import org.apache.spark.sql.SparkSession

/**
 * Created by shakeer on Jan, 2021
 **/
object MapRDD {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local")
      .appName("Map on RDD")
      .getOrCreate()

    val data=spark.read.textFile("src/main/resources/emp.txt").rdd
    val op=data.map(x=>{
      val s=x.trim().split(",")
      val empno=s(0)
      val ename=s(1).toLowerCase
      val sal=s(2).toInt
      val grade =if (sal>5000) "A" else
        if (sal >3000) "B" else
        if (sal > 2000) "C" else "D"
      val deptno=s(3).toInt
      val dname=deptno match {
        case 10 =>"Finance"
        case 20 =>"Software"
        case 30 => "IT"
        case 40 => "Agg"
        case others =>"others"
      }
      List(empno,ename,s(2),grade,dname).mkString("\t")
    })
    op.collect.foreach(println)
  }
}
