package spark.wiki

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.ListMap

/**
 * Object which contains methods for parsed Wiki files analysis
 */
object WikipediaDumpAnalyzer {

  /**
   * Load content of parsed files to RDD[(String, String)]
   * @param dumpFilesLocation - root location for input parsed files
   * @return RDD with tuples (String, String). 1st parameter is file path, 2nd - file content
   */
  def getDataFromFiles(dumpFilesLocation: String): RDD[(String, String)] = {
    val appName: String = "Wiki analysis"
    val master: String = "local[*]"
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.wholeTextFiles(s"$dumpFilesLocation/*.xml")
  }

  /**
   * Method to analyze RDD with wikipedia dump and return map of top 10 words used
   * @param data - RDD[(String, String)] with parsed wikipedia dump
   * @return - Map[String, Int] with pairs "word" -> count
   */
  def getTop10WordsHeatMap(data: RDD[(String, String)]): Map[String, Int] = {
    val countedData = data.map{
      case (_, value) => getWordsHeatMap(getTagContentFromXML(value))
    }
    ListMap(countedData.reduce(combineTwoMaps).toSeq.sortWith(_._2 > _._2).take(10):_*)
  }

  /**
   * Method to analyze RDD with wikipedia dump and return Map of biggest 10 pages
   * @param data - RDD[(String, String)] with parsed wikipedia dump
   * @return - Map[String, String] with pairs "page file path" -> "page file content"
   */
  def getTop10BiggestPages(data: RDD[(String, String)]): Map[String, String] = ListMap(data.top(10)(Ordering.by(page => getTagContentFromXML(page._2).length)):_*)

  private[wiki] def getTagContentFromXML(xml: String, tagName: String = "text"): String = {
    val openTag = s"<$tagName>"
    val closeTag = s"</$tagName>"
    val startIndex = xml.indexOf(openTag)
    val endIndex = xml.indexOf(closeTag)
    if (startIndex == -1 || endIndex == -1) ""
    else xml.substring(startIndex + openTag.length, endIndex)
  }

  /**
   * Method to merge two maps in one. Values are summarized for same keys
   * @param x - first Map[String, Int]
   * @param y - second Map[String, Int]
   * @return - result Map[String, Int] with merged content of input maps
   */
  private[wiki] def combineTwoMaps(x: Map[String, Int], y: Map[String, Int]): Map[String, Int] = {
    x++y.map{case (key, value) => key -> (value + x.getOrElse(key, 0))}
  }

  /**
   * Method to get words heat map for text content of one page file
   * @param text - text content of one page file
   * @return - Map[String, Int] with pairs "word" -> count
   */
  private[wiki] def getWordsHeatMap(text: String): Map[String, Int] = {
    val replaceStr = "\\{[\\s\\S\\d\\D\\n]*?}|ref"
    val splitStr = "[^\\d\\w']+"
    text.replaceAll(replaceStr, "").split(splitStr).groupBy(_.toLowerCase).map{case (word, qty) => (word, qty.length)}
  }
}
