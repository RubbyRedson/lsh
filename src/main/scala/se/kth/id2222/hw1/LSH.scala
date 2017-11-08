package se.kth.id2222.hw1

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by Nick on 11/6/2017.
  */
object LSH {

  val SHINGLE_LENGTH = 5
  val PRIME = 2147483647
  val SIMILARITY_THRESHOLD = 0.6
  val BAND_SIMILARITY_THRESHOLD = 0.1
  val NUMBER_OF_BANDS = 10
  val NUMBER_OF_ROWS = 5
  val NUMBER_OF_HASHFUNCTIONS = 50

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\winutils")

    val sc = new SparkContext("local[8]", "LSH")
    val sharedHashFunctions = sc.broadcast(getHashFunctions(NUMBER_OF_HASHFUNCTIONS))

    val textFile = sc.textFile("./dataset/SMSSpamCollection.txt")
    val documents : RDD[(String, Long)] = textFile.zipWithIndex()
    val preprocessed : RDD[(Long, String)] = documents.map { case (document, id) => (id, preprocessDocument(document)) }
    val shingled : RDD[(Long, TraversableOnce[Int])] = preprocessed.map {case (key, document) => (key, shingle(document))}
    val minHashed = shingled.map {case (key, shingleList) => (key, minHash(shingleList, sharedHashFunctions.value))}

    val similarities : RDD[(Long, Long, Double)] =
      minHashed.cartesian(minHashed)
        .filter{ case ((key, _),(key2, _)) => key > key2}
        .map{case ((key, signature),(key2, signature2)) => (key, key2, findSimilarity(signature, signature2))}
        .filter{case (_, _, similarity) => similarity > SIMILARITY_THRESHOLD}

    val bands : RDD[(Long, TraversableOnce[Int])] = minHashed.map{case (key, signature) => (key, signatureToHashedBandsOfRows(signature.toList,
      NUMBER_OF_BANDS, NUMBER_OF_ROWS))}

    val lshResult = bands.cartesian(bands)
      .map{case ((key1, bands1),(key2, bands2)) => (key1, key2, findSimilarity(bands1, bands2))}
      .filter{case (_, _, similarity) => similarity > BAND_SIMILARITY_THRESHOLD}

    lshResult.saveAsTextFile("./target/results" + System.currentTimeMillis())

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

  def signatureToHashedBandsOfRows(signature: List[Int], numberOfBands : Int, numberOfRowsInBand : Int) : List[Int] = {
    if (signature.size != numberOfBands * numberOfRowsInBand)
      throw new IllegalArgumentException("Wrong arguments number of bands times number of rows should equal length of signature")
    var i = 0
    val bands = new ArrayBuffer[List[Int]]()
    while (i + numberOfRowsInBand <= signature.length) {
      bands.append(signature.slice(i, i + numberOfRowsInBand))
      i += numberOfRowsInBand
    }

    bands.map(band => band.hashCode()).toList
  }
}
