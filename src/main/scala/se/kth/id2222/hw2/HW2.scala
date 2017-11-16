package se.kth.id2222.hw2

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object HW2 {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\winutils")

    val sc = new SparkContext("local[8]", "Frequent Itemsets")
    val textFile = sc.textFile("./dataset/T10I4D100K.dat")
    val transactions: RDD[(String, Long)] = textFile.zipWithIndex()

    val numberOfTransactions = transactions.count()

    val s = 800
    val c = 0.5

    //simple(transactions, s.toInt, c)
    complex(transactions, s, c)

  }

  def simple(transactions: RDD[(String, Long)], s: Int, c: Double): Unit = {

    // First we find the frequent items in the transactions
    val frequentItems =
      transactions
        .flatMap { case (transaction, _) => emitItems(transaction) }
        .reduceByKey(_ + _)
        .map { case (transaction, count) => (count, transaction) }
        .filter { case (support, _) => support > s }
        .sortByKey(false)


    frequentItems.top(10).foreach(println)
    val frequentItemsAsSet = frequentItems.map { case (_, item) => item }.collect().toSet

    val frequentPairs = transactions
      .flatMap { case (transaction, _) => emitDuos(transaction, frequentItemsAsSet) }
      .reduceByKey(_ + _)
      .map { case (transaction, count) => (count, transaction) }
      .filter { case (support, _) => support > s }
      .sortByKey(false)

    frequentPairs.top(10).foreach(println)
    println(frequentPairs.count())

    frequentItems.cache()
    frequentPairs.cache()

    val associationRulesPairToItem = frequentPairs.cartesian(frequentItems)
      .filter { case ((_, pair), (_, item)) => pair.split(" ").contains(item) }
      .filter { case ((pairSpport, pair), (itemSupport, item)) => pairSpport / itemSupport.toDouble > c }
      .map { case ((pairSpport, pair), (itemSupport, item)) => (pairSpport / itemSupport.toDouble, pair.split(" ").filter(itemInPair => item != itemInPair).head + " => " + item) }
      .sortByKey(false)

    associationRulesPairToItem.foreach(println)

    //    transactions.collect().foreach(println)
    //lshResult.saveAsTextFile("./target/results" + System.currentTimeMillis())
  }

  def complex(transactions: RDD[(String, Long)], s: Int, c: Double) = {
    val frequentItems =
      transactions
        .flatMap { case (transaction, _) => emitItems(transaction) }
        .reduceByKey(_ + _)
        .map { case (transaction, count) => (count, transaction) }
        .filter { case (support, _) => support > s }
        .sortByKey(false)


    frequentItems.top(10).foreach(println)
    val frequentItemsAsSet = frequentItems.map { case (_, item) => item }.collect().toSet

    val frequentItemsets = transactions.flatMap { case (transaction, _) => emitItemsets(transaction, 3, frequentItemsAsSet) }
      .map { itemSet => (itemSet.toSeq.sorted.fold(z = "")((z, a) => z.trim() + " " + a), 1) }
      .reduceByKey(_ + _)
      .map { case (transaction, count) => (count, transaction) }
      .filter { case (support, _) => support > s }
      .sortByKey(false)

    val frequentItemsetsAsMap: mutable.Map[String, Int] = new mutable.HashMap[String, Int]()
    frequentItemsets.collect().foreach { case (support: Int, itemset: String) => {
      frequentItemsetsAsMap.put(itemset.trim(), support)
    }
    }

    frequentItemsets
      .filter { case (_, itemset) => itemset.trim().split(" ").length > 1 }
      .flatMap { case (support, itemset) => checkConfidence(itemset, support, frequentItemsetsAsMap.toMap, c) }
      .sortByKey(false)
      .foreach(println)

  }

  def emitItems(transaction: String): TraversableOnce[(String, Int)] = {
    transaction.split(" ").map(item => (item, 1))
  }

  def emitDuos(transaction: String, setOfFrequentItems: Set[String]): TraversableOnce[(String, Int)] = {
    val frequent = transaction
      .split(" ")
      .filter(item => setOfFrequentItems.contains(item))

    val result = new ArrayBuffer[(String, Int)]()
    frequent.foreach(item => frequent.foreach(secondItem => if (secondItem < item) result.append((item + " " + secondItem, 1))))
    result
  }

  def emitItemsets(transaction: String, maxLength: Int, setOfFrequentItems: Set[String]): TraversableOnce[Set[String]] = {
    val result = new mutable.HashSet[Set[String]]()
    for (i <- 1 to maxLength) {
      emitItemset(transaction, i, setOfFrequentItems.filter(item => transaction.split(" ").contains(item))).foreach(itemset => result.add(itemset))
    }
    result
  }

  def emitItemset(transaction: String, length: Int, setOfFrequentItems: Set[String]): TraversableOnce[Set[String]] = {
    if (length == 1) return emitItems(transaction).map { case (str, int) => str.split(" ").toSet }
    if (length == 2) return emitDuos(transaction, setOfFrequentItems).map { case (str, int) => str.split(" ").toSet }

    val pairs = emitDuos(transaction, setOfFrequentItems)
    val items = emitItems(transaction)
    var currentSize = 2

    var itemsets = new ArrayBuffer[Array[String]]()
    pairs.foreach(pair => itemsets.append((pair._1.split(" "))))
    while (currentSize < length) {
      var newItemsets = new ArrayBuffer[Array[String]]()
      itemsets.foreach(itemset => setOfFrequentItems.filter(item => !itemset.contains(item)).foreach(item => {
        newItemsets.append(itemset :+ item)
      }))
      if (newItemsets.nonEmpty) {
        currentSize += 1
        itemsets = newItemsets
      }
      else { //terminate if we are out of items
        currentSize = length
        itemsets = newItemsets
      }
    }
    val result = new mutable.HashSet[Set[String]]()
    itemsets.foreach(itemset => result.add(itemset.toSet))
    result
  }

  def checkConfidence(itemset: String, support: Int, mapOfItemsets: Map[String, Int], confidenceLevel: Double): TraversableOnce[(Double, String)] = {
    val result = new ArrayBuffer[(Double, String)]()
    itemset.split(" ").foreach(item => {
      val itemsetWithoutItem = itemset.split(" ").filter(item1 => item1 != item && !item.isEmpty && !item1.isEmpty)
      val subsetAsStr = itemsetWithoutItem.toSeq.sorted.fold(z = "")((z, a) => z.trim() + " " + a)
      if (itemsetWithoutItem.length > 0 && mapOfItemsets.contains(subsetAsStr.trim())) {
        val supportOfSubset = mapOfItemsets(subsetAsStr.trim())
        if (support / supportOfSubset.toDouble > confidenceLevel) result.append((support / supportOfSubset.toDouble, subsetAsStr + " => " + item))
      }
    })
    result
  }

}
