import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// Define case classes to represent the data
//case class Transaction(customer_id: Int, transaction_date: String, transaction_type: String, amount: Double)
//case class CustomerTransactionFrequency(customer_id: Int, transaction_frequency: Long)
//case class TransactionPattern(customer_id: Int, transaction_date: String, transaction_type: String, amount: Double)

object BankDataAnalysisDF {

  // Create a SparkSession
  val spark: SparkSession = SparkSession.builder()
    .appName("BankDataAnalysis")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Function to load the bank dataset from the CSV file into a DataFrame
  def loadDataFromCSV(filename: String): DataFrame = {
    spark.read.option("header", "true").csv(filename).selectExpr(
      "CAST(customer_id AS INT)", "transaction_date", "transaction_type", "CAST(amount AS DOUBLE)"
    )
  }

  // Exploring schema and first rows
  def exploreDataset(data: DataFrame) = {
    data.printSchema()
    data.show(2)
  }

  // Function to handle missing or erroneous data and return a cleaned DataFrame
  def handleMissingData(data: DataFrame): DataFrame = {
    data.filter($"amount" > 0)
  }

  // Function to calculate and display basic statistics
  def calculateBasicStatistics(data: DataFrame): Unit = {
    val totalDeposits = data.filter($"transaction_type" === "deposit").agg(sum("amount")).collect()(0)(0)
    val totalWithdrawals = data.filter($"transaction_type" === "withdrawal").agg(sum("amount")).collect()(0)(0)
    val averageTransactionAmount = data.agg(mean("amount")).collect()(0)(0)

    println(s"Total Deposits: $totalDeposits")
    println(s"Total Withdrawals: $totalWithdrawals")
    println(s"Average Transaction Amount: $averageTransactionAmount")
  }

  // Function to determine the number of unique customers and the frequency of transactions per customer
  def customerTransactionFrequency(data: DataFrame): DataFrame = {
    val transactionCounts = data.groupBy("customer_id").agg(count("customer_id").alias("transaction_frequency"))
    transactionCounts
  }

  // Function to group transactions based on the transaction date
  def groupByTransactionDate(data: DataFrame): DataFrame = {
    data.groupBy("transaction_date").agg(sum("amount").alias("total_amount"))
  }

  // Function to segment customers based on their transaction behavior
  def customerSegmentation(data: DataFrame): DataFrame = {
    data.select($"customer_id", when($"amount" >= 1000, "high-value").otherwise("regular").alias("segment"))
  }

  // Function to calculate the average transaction amount for each customer segment
  def calculateAvgTransactionAmount(data: DataFrame, segments: DataFrame): DataFrame = {
    val joinedData = data.join(segments, Seq("customer_id"), "inner")
    val avgTransactionAmounts = joinedData.groupBy("segment").agg(avg("amount").alias("average_transaction_amount"))
    avgTransactionAmounts
  }

  // Function to identify patterns in customer transactions
  def identifyTransactionPatterns(data: DataFrame): DataFrame = {
    data
  }
  // visualization
  def visualizeTransactionPatterns(data: DataFrame): DataFrame = {
    data
  }

  def main(args: Array[String]): Unit = {
    val data = loadDataFromCSV("./transaction1.csv")
    println("Schema and first 2 rows")
    exploreDataset(data)
    val data_clean = handleMissingData(data)
    calculateBasicStatistics(data_clean)
    val transaction_frequency = customerTransactionFrequency(data_clean)
    println("TRANSACTION FREQUENCY")
    transaction_frequency.show()
    customerTransactionFrequency(data_clean).show()
    println("***************")
    val data_grouped_transaction_date = groupByTransactionDate(data_clean)
    println("data_grouped_transaction_date")
    data_grouped_transaction_date.show()
    val customers_segmentation = customerSegmentation(data_clean)
    println("customers_segmentation")
    customers_segmentation.show()
    val average_transaction_amount = calculateAvgTransactionAmount(data_clean, customers_segmentation)
    println("average_transaction_amount")
    average_transaction_amount.show()
    val transaction_patterns = identifyTransactionPatterns(data_clean)
    println("transaction_patterns")
    transaction_patterns.show()
    identifyTransactionPatterns(data_clean).show()
    visualizeTransactionPatterns(data_clean).show()

  }
}
