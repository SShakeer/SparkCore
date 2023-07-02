import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created by shakeer on Jan, 2021
 **/
object joinsDemo {
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local")
      .getOrCreate()
    val empdata=spark.read.textFile("src/main/resources/emp.txt").rdd
    //empno,ename,sal,deptno
    val deptdata=spark.read.textFile("src/main/resources/dept.txt").rdd
    //deptno,dname,location

    val e = empdata.map{ x =>
      val data = x.split(",")
      val deptno = data(3).toInt
      val empno = data(0)
      val ename = data(1)
      val sal   = data(2).toInt
      val info = empno+","+ename+","+sal
      (deptno,info)
    }

    e.collect.foreach(println)

    val d = deptdata.map{ x =>
      val w = x.split(",")
      val deptno = w(0).toInt
      val info = w(1)+","+w(2)
      (deptno,info)
    }

    d.collect.foreach(println)
    val ed =  e.join(d)
    ed.collect.foreach(println)

    val ed2 = ed.map{x =>
      val einfo = x._2._1
      val dinfo = x._2._2
      val info = einfo+","+dinfo
      info
    }

    ed2.saveAsTextFile("src/main/resources/empoutput.txt")
  }
}
