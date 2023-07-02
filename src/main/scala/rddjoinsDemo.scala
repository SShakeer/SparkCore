import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created by shakeer on Jan, 2021
 **/
object rddjoinsDemo {
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local")
      .getOrCreate()
    val empdata=spark.read.textFile("src/main/resources/emp.txt").rdd
    //empno,ename,sal,deptno
    val deptdata=spark.read.textFile("src/main/resources/dept.txt").rdd
    //deptno,dname,loc
    //select e.ename,e.sal,e.empno,d.dname,d.locatio from emp e ,dept d where e.deptno=d.deptno;
    val emp_info=empdata.map{x=>
      val data=x.split(",")
      val deptno=data(3).toInt
      val empno=data(0).toInt
      val ename=data(1)
      val sal=data(2).toInt
      val columns=empno+","+ename+","+sal
      (deptno,columns)
    }
    emp_info.collect.foreach(println)

    val dept_info=deptdata.map{x=>
      val data2=x.split(",")
      val deptno=data2(0).toInt
      val dname=data2(1)
      val location=data2(2)
      val dcols=dname+","+location
      (deptno,dcols)
    }
    dept_info.collect.foreach(println)
    //join
    val joins=emp_info.join(dept_info)
    joins.collect.foreach(println)


    val finaloutput=joins.map{x=>
      val emp=x._2._1
      val dept=x._2._2
      val total=emp+","+dept
      total
    }
    finaloutput.saveAsTextFile("src/main/resources/output1")
  }
}
