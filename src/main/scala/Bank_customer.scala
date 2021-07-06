import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.functions._

object Bank_customer {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    case class user_info(id:Int,first_name:String,Last_name:String,email:String,gender:String,ip_address:String)
    case class tran_info(id:Int,first_name:String,Phone_number:Long,trans_ID:String,CreditCardDetails:Double)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("bank_customer")
      .getOrCreate()

    val userDF = spark.read.option("inferschema","true")
      .option("header","true")
      .csv("datasets/MOCK_DATA.csv")

    userDF.show()

    val transectionDF = spark.read.option("inferschema","true")
      .option("header","true")
      .csv("datasets/t_DATA.csv")

    transectionDF.show()

    val joinCondition = userDF.col("id") === transectionDF.col("id")
    val jointype = "inner"
    val joinDF = userDF.join(transectionDF,joinCondition,jointype)
      .select("trans_ID","first_name","gender","CreditCardDetail","Phone_number")

    val finalDF = joinDF.withColumn("CreditCardDetails",
      regexp_replace(joinDF("CreditCardDetail"),"[0-9]{12}","XXXX-XXXX-XXXX-"))
      .withColumn("Phone",
        regexp_replace(joinDF("Phone_number"),"[0-9]{6}","XXX-XXX-"))
      .drop("CreditCardDetail").drop("Phone_number")

    finalDF.show(false)

    "Show only 4 digit of Creditcardnumber rest of number is hided by XXXX-XXXX-XXXX-"

  }

}
