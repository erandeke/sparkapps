import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

// Big Data Use case to find top customer shopping expenditure

/**
 * @author Kedar Erande
 */
object TotalCustomerExpenditure {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "TotalCustomerExpenditure")

    val customerData = sc.textFile("/Users/kedarerande/Downloads/cust-orders.csv")

    val transformed = customerData.map(x => (x.split(",")(0), x.split(",")(2).toFloat)) //key,value

    val totalByCustomer = transformed.reduceByKey((x, y) => (x + y))

    val topExpenditure = totalByCustomer.sortBy(x => x._2)

    val result = topExpenditure.collect() //result is not rdd its a local variable on memory

    result.foreach(println)


  }

}
