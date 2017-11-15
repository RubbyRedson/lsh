package se.kth.id2222.hw2

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object HW2 {

  val s = 0.03
  val c = 0.003

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\winutils")

    val sc = new SparkContext("local[8]", "Frequent Itemsets")
    val textFile = sc.textFile("./dataset/T10I4D100K.dat")
    val transactions : RDD[(String, Long)] = textFile.zipWithIndex()

    val numberOfTransactions = transactions.count()

    // First we find the frequent items in the transactions
    val frequentItems =
      transactions
      .flatMap { case (transaction, _) => emitItems(transaction) }
      .reduceByKey(_ + _)
      .map { case (transaction, count) => (count / numberOfTransactions.toDouble, transaction) }
      .filter{ case (frequency, _) => frequency > s }
      .sortByKey(false)


    frequentItems.top(10).foreach(println)
    val frequentItemsAsSet = frequentItems.map{case (_, item) => item}.collect().toSet

    val frequentPairs = transactions
      .flatMap { case (transaction, _) => emitDuos(transaction, frequentItemsAsSet) }
      .reduceByKey(_ + _)
      .map { case (transaction, count) => (count / numberOfTransactions.toDouble, transaction) }
      .filter{ case (frequency, _) => frequency > c}
      .sortByKey(false)

    frequentPairs.top(10).foreach(println)
    println(frequentPairs.count())


//    transactions.collect().foreach(println)
    //lshResult.saveAsTextFile("./target/results" + System.currentTimeMillis())

  }

  def emitItems(transaction : String) : TraversableOnce[(String, Int)] = {
    transaction.split(" ").map(item => (item, 1))
  }

  def emitDuos(transaction : String, setOfFrequentItems : Set[String]) : TraversableOnce[(String, Int)] = {
    transaction
      .split(" ")
      .filter(item => setOfFrequentItems.contains(item))
      .sliding(2, 1)
      .filter(pair => pair.length > 1)
      .map(pair => (pair(0) +  " " + pair(1), 1))
  }

}
