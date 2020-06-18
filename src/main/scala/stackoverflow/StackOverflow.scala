package stackoverflow

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import annotation.tailrec

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable

/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())
    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}


/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */
  // Question#1
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    // Make RDD with questions and after map it to get the key for join
    val question_rdd = postings.filter(post => post.postingType == 1).map(post => (post.id, post))
    // Make RDD with answers and after map it to get the key for join
    val answer_rdd   = postings.filter(post => post.postingType == 2).map(post => (post.parentId.get, post))
    // Do inner join to connect questions and answers and after group it to get RDD with all answers for each question
    question_rdd.join(answer_rdd).groupByKey()
  }

  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
          var i = 0
          while (i < as.length) {
            val score = as(i).score
                if (score > highScore)
                  highScore = score
                  i += 1
          }
      highScore
    }
    // Question#2
    grouped
      // get only values from RDD
      .values
      // map to RDD to get in format as question and all answers for it as sequence
      .map(qa => (qa.head._1, qa.map(_._2)))
      // change structure RDD to question and high score. Get highscore from recursive function
      .map( { case (q, answers) => (q, answerHighScore(answers.toArray)) })
  }

  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }
    // Question#3
    scored
      // Change RDD structure to vector of language index and rating
      .map({ case (post, rating) => (firstLangInTag(post.tags, langs), rating) })
      // Check that language index isn't empty because function firstLangInTag might return None value
      .filter(xs => xs._1.nonEmpty)
      // Multiply index of language to landSpread as we need it in the assignment
      .map({ case (tag, rating) => (tag.get * langSpread, rating) })
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    val newMeans = means.clone() // you need to compute newMeans

    // TODO: Fill in the newMeans array
    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  def medianVectors(s: Seq[Double]) = {
    val (lower, upper) =
      s.sortWith(_ < _).
        splitAt(s.size / 2)
    if (s.size % 2 == 0)
      (lower.last + upper.head) / 2.0
    else upper.head
  }


  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey()

    // Question#4

    val median = closestGrouped.mapValues { vs =>

      // firstly find dominant language index so we need it for language label and language percent
      val langIndex: Int =
      // group by language index
        vs.groupBy(xs => xs._1)
          // find how much entries exist for each language index
          .map(xs => (xs._1, xs._2.size))
          // get dominant language with function maxBy and then devide on langSpead to get original index
          .maxBy(xs => xs._1)._1 / langSpread

      // get name of language by index that we get above
      val langLabel: String = langs(langIndex)

      // get cluster size from size of array vs
      val clusterSize: Int = vs.size

      // get percent of the questions in the most common language
      val langPercent: Double =
      // trasform to get only language index
        vs.map( { case (v1, v2) => v1 })
          // firstly filter, what language is dominant. After get it percent in the cluster.
          .filter(v1 => v1 == langIndex * langSpread).size / clusterSize * 100

      // Get median score. Pass to function medianVectors highscore as sequence and then transform it to Int
      val medianScore: Int    = medianVectors(vs.map(_._2.toDouble).toSeq).toInt

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}

/**

1. Do you think that partitioning your data would help?

Answer: It can helps to speed up program implementation if configure manualy count of partitions depends on
cluster configuration and use maybe use range partitioning.
-----------------------------------------------------------------------------------------------------------------------------------------------

2. Have you thought about persisting some of your data? Can you think of why persisting your data in memory may be helpful for this algorithm?

Answer: I think that if persist (cache) some data of medium stages it helps to increase speed of implementation program.
-----------------------------------------------------------------------------------------------------------------------------------------------

3. Of the non-empty clusters, how many clusters have "Java" as their label (based on the majority of questions, see above)? Why?

Answer: Two non-empty clusters have "Java" as their label.
-----------------------------------------------------------------------------------------------------------------------------------------------

4.Only considering the "Java clusters", which clusters stand out and why?

Answer: There are Java cluster that have 114.963 questions and median 0 score. But on the other side there are
java cluster with 2 times less count of questions and median 2 score. I guess it happens because in first case
there are hard questions, questions with bad description and questions from beginners.
-----------------------------------------------------------------------------------------------------------------------------------------------

5. How are the "C# clusters" different compared to the "Java clusters"?

Answer: С# clusters have more higher scores than Java clusters and C# count of questions is little bit more.
I suggest it means that C# have easier questions.
  **/