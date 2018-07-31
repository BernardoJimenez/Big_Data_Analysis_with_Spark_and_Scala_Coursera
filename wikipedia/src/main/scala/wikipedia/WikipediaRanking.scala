package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf()
                            .setMaster("local[*]")
                            .setAppName("wikiRank")
  val sc: SparkContext = new SparkContext(conf)
  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(x => WikipediaData.parse(x)).cache()

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    // 0 initial value, for each article add 1 if language is found, then add all found together.
    // Need "sum" varaible to hold the 1's we add together
    rdd.aggregate(0)((sum, article) => if (article.mentionsLanguage(lang)) sum+1 else sum, _+_)
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    // take each langauge in the list and convert it to a tuple of (language, #occurences) then
    // order this in decreasing ordering using the second element in each tuple by using '_._2'
    val ranks = langs.map(lang => (lang, occurrencesOfLang(lang, rdd))).sortWith(_._2 > _._2)
    ranks
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    // a conditional in a map returns an Option[] or None, so a flatMap flattens this to remove the Option layer
    // each article is transformed to a tuple where the first element is the language in the langs list and the
    // second element is an article. Group by key puts all articles in an iterable for each unique language key 
    // in the pairs created.
    val indexedRDD = rdd.flatMap(article => for(lang <- langs if article.mentionsLanguage(lang)) yield (lang, article))
                      .groupByKey()
    indexedRDD
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    // transform each (language, articlesIterable) pair into another tuple pair with language and
    // key being the number of articles by taking the size of the iterable. Then use collect() to 
    // return all elements of the RDD as an array to the driver and convert to a list. Finally, sort
    // by the number of articles for each language in descending order
    val ranks = for ((lang, list) <- index) yield (lang, list.size)
    ranks.collect().toList.sortWith(_._2 > _._2)
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    // flattening with map gets rid of Option[] layer obtained from a conditional in a map. the for loop returns
    // only those languages meeting the condition of being mentioned, and adds it as a tuple (lang, 1) for each
    // occurrence for each article. These get added together with reduce to reduce all seen instances of a language
    // to a single tuple sum (lang, sum). This is then converted to an array on the driver with collect, converted
    // to a list, and sorted by the second element in the tuple, the count, in decreasing order.
    val indexedRDD = rdd.flatMap(article => for(lang <- langs if article.mentionsLanguage(lang)) yield (lang, 1))
    indexedRDD.reduceByKey(_+_).collect().toList.sortWith(_._2 > _._2)
  }

  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
