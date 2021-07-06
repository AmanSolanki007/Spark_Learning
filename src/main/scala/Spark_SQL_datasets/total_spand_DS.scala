package Spark_SQL_datasets
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType,IntegerType,StructType}
import org.apache.log4j._

object total_spand_DS {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("total_spand")
      .getOrCreate()

    val schema = new StructType().add("cust_ID",IntegerType,nullable = false)
      .add("customer_no",IntegerType,nullable = false)
      .add("expenditure",FloatType,nullable = false)

    val data = spark.read.schema(schema).csv("datasets/customer-orders.csv")

    data.select("cust_ID","expenditure")
      .groupBy("cust_ID")
      .sum("expenditure")
      .orderBy("cust_ID").show()
  }


}
