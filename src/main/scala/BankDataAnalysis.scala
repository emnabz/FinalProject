import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


// Define case classes to represent the data
case class Transaction(customer_id: Int, transaction_date: String, transaction_type: String, amount: Double)
case class CustomerTransactionFrequency(customer_id: Int, transaction_frequency: Long)
case class TransactionPattern(customer_id: Int, transaction_date: String, transaction_type: String, amount: Double)

object BankDataAnalysis {

  // Function to load the bank dataset from the CSV file into an RDD
  def loadDataFromCSV(sc: SparkContext, filename: String): RDD[Transaction] = {
    val dataRDD = sc.textFile(filename)
    dataRDD.map(line => {
      val fields = line.split(",")
      Transaction(fields(0).toInt, fields(1), fields(2), fields(3).toDouble)
    })
  }


  // Function to handle missing or erroneous data and return a cleaned RDD
  def handleMissingData(data: RDD[Transaction]): RDD[Transaction] = {
    // Implement your logic here to handle missing or erroneous data
    // For example, you can use RDD transformations like filter or map to clean the data
    // Return the cleaned RDD
    data.filter(transaction => transaction.amount > 0)
  }

  // Function to calculate and display basic statistics
  def calculateBasicStatistics(data: RDD[Transaction]): Unit = {
    val totalDeposits = data.filter(_.transaction_type == "deposit").map(_.amount).sum()
    val totalWithdrawals = data.filter(_.transaction_type == "withdrawal").map(_.amount).sum()
    val averageTransactionAmount = data.map(_.amount).mean()

    println(s"Total Deposits: $totalDeposits")
    println(s"Total Withdrawals: $totalWithdrawals")
    println(s"Average Transaction Amount: $averageTransactionAmount")
  }

  // Function to determine the number of unique customers and the frequency of transactions per customer
  def customerTransactionFrequency(data: RDD[Transaction]): RDD[CustomerTransactionFrequency] = {
    val transactionCounts = data.map(transaction => (transaction.customer_id, 1))
      .reduceByKey(_ + _)

    transactionCounts.map { case (customer_id, frequency) =>
      CustomerTransactionFrequency(customer_id, frequency)
    }
  }

  // Function to group transactions based on the transaction date
  def groupByTransactionDate(data: RDD[Transaction]): RDD[(String, Double)] = {
    // Implement your logic here to group transactions based on the transaction date (daily, monthly, etc.)
    // For example, you can use RDD transformations like map and reduceByKey to aggregate transactions by date
    // Return an RDD with the aggregated data, where the key is the date and the value is the total transaction amount
    data.map(transaction => (transaction.transaction_date, transaction.amount))
      .reduceByKey(_ + _)
  }



  // Function to segment customers based on their transaction behavior
  def customerSegmentation(data: RDD[Transaction]): RDD[(Int, String)] = {
    // Implement your logic here to segment customers based on transaction behavior (e.g., high-value customers, frequent transactors, inactive customers)
    // You can use RDD transformations like map to label customers with segments
    // Return an RDD with customer ID and corresponding segment label
    data.map(transaction => (transaction.customer_id, "high-value")) // Example: Assign all customers as "high-value" for simplicity
  }

  // Function to calculate the average transaction amount for each customer segment
  def calculateAvgTransactionAmount(data: RDD[Transaction], segments: RDD[(Int, String)]): RDD[(String, Double)] = {
    // Implement your logic here to calculate the average transaction amount for each customer segment
    // You can use RDD transformations like join and aggregateByKey to calculate the average transaction amount
    // Return an RDD with segment labels and corresponding average transaction amount
    val customerAmounts = data.map(transaction => (transaction.customer_id, transaction.amount))
    val joinedData = customerAmounts.join(segments)
    val avgTransactionAmounts = joinedData.map { case (_, (amount, segment)) => (segment, amount) }
      .aggregateByKey((0.0, 0L))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      )
      .mapValues { case (totalAmount, count) => totalAmount / count.toDouble }

    avgTransactionAmounts
  }

  // Function to identify patterns in customer transactions
  def identifyTransactionPatterns(data: RDD[Transaction]): RDD[TransactionPattern] = {
    // Implement your logic here to identify patterns in customer transactions (e.g., large deposits followed by large withdrawals)
    // You can use RDD transformations like window functions or groupBy to analyze the transaction patterns
    // Return an RDD with relevant information (e.g., customer ID, transaction date, transaction type, amount) for identified patterns
    data // For simplicity, this example function returns the original data as TransactionPattern RDD
      .map(transaction => TransactionPattern(transaction.customer_id, transaction.transaction_date, transaction.transaction_type, transaction.amount))
  }


  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "BankDataAnalysis")
    val data = loadDataFromCSV(sc, "./transaction.csv")
    val data_clean = handleMissingData(data)
    calculateBasicStatistics(data_clean)
    val transaction_frequency = customerTransactionFrequency(data_clean)
    println("TRANSACTION FREQUENCY")
    transaction_frequency.collect().foreach(println)
    println("***************")
    val data_grouped_transaction_date = groupByTransactionDate(data_clean)
    println("data_grouped_transaction_date")
    data_grouped_transaction_date.collect().foreach(println)
    val customers_segmentation = customerSegmentation(data_clean)
    println("customers_segmentation")
    customers_segmentation.collect().foreach(println)
    val Average_transaction_amount = calculateAvgTransactionAmount(data_clean, customers_segmentation)
    println("Average_transaction_amount")
    Average_transaction_amount.collect().foreach(println)
    val transaction_patterns = identifyTransactionPatterns(data_clean)
    println("transaction_patterns")
    transaction_patterns.collect().foreach(println)
  }

}