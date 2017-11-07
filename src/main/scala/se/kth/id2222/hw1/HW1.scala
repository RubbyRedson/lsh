package se.kth.id2222.hw1

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by Nick on 11/6/2017.
  */
object HW1 {

  val SHINGLE_LENGTH = 9
  val PRIME = 2147483647


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\winutils")

    val sc = new SparkContext("local[*]", "TestProgram")
    val sharedHashFunctions = sc.broadcast(getHashFunctions(5))

    val textFile = sc.textFile("./dataset/SMSSpamCollection.txt")
    val documents : RDD[(String, Long)] = textFile.zipWithIndex()
    val preprocessed : RDD[(Long, String)] = documents.map { case (document, id) => (id, preprocessDocument(document)) }
    val shingled : RDD[(Long, TraversableOnce[Int])] = preprocessed.map {case (key, document) => (key, shingle(document))}
    val minHashed = shingled.map {case (key, shingleList) => (key, minHash(shingleList, sharedHashFunctions.value))}
    val similaritites : RDD[(Long, Long, Double)] =
      minHashed.cartesian(minHashed)
        .filter{ case ((key, _),(key2, _)) => key > key2}
          .map{case ((key, signature),(key2, signature2)) => (key, key2, findSimilarity(signature, signature2))}

    similaritites.saveAsTextFile("./target/results" + System.currentTimeMillis())
  }

  def preprocessDocument(document : String) : String = {
    if (document.startsWith("ham")) {
      document.replaceFirst("ham\t", "").toLowerCase().replaceAll("[^\\w\\s]", "").replaceAll("\\s+", " ")
    } else if (document.startsWith("spam")) {
      document.replaceFirst("spam\t", "").trim().toLowerCase().replaceAll("[^\\w\\s]", "").replaceAll("\\s+", " ")
    } else {
      document.trim().toLowerCase().replaceAll("[^\\w\\s]", "").replaceAll("\\s+", " ")
    }
  }

  def shingle(document : String) : TraversableOnce[Int] = {
    val resultingList = new ArrayBuffer[Int]()
    var i = 0
    while (i + SHINGLE_LENGTH < document.length()) {
      resultingList += document.substring(i, i + SHINGLE_LENGTH).hashCode()
      i += 1
    }
    resultingList
  }

  def minHash(listOfShingles : TraversableOnce[Int], hashFunctions : List[Int => Int]) : TraversableOnce[Int] = {
    var ind = 0
    val result = new ArrayBuffer[Int]
    while (ind < hashFunctions.length) {
      var min = Int.MaxValue
      for (shingle <- listOfShingles) {
        val hashResult = hashFunctions(ind).apply(shingle)
        if (hashResult < min) min = hashResult
      }
      result.append(min)
      ind += 1
    }
    result
  }

  def getHashFunctions(number : Int) : List[Int => Int] = {
    val result = new ArrayBuffer[Int => Int]()
    val rand = new Random()
    var ind = 0
    while (ind < number) {
      val a = rand.nextInt()
      val b = rand.nextInt()
      result.append((x) => ((a.toLong * x + b) % PRIME).toInt)
      ind += 1
    }
    result.toList
  }

  def findSimilarity(ints: TraversableOnce[Int], ints1: TraversableOnce[Int]) : Double = {
    val intersection = ints.toIndexedSeq.intersect(ints1.toIndexedSeq)
    val union = ints.toIndexedSeq.union(ints1.toIndexedSeq).distinct
    intersection.size.toDouble / union.size
  }
}
