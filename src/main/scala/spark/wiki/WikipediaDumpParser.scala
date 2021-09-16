package spark.wiki

import scala.io.Source
import scala.xml.pull._
import scala.collection.mutable.ArrayBuffer
import java.io.File
import java.io.FileOutputStream
import scala.xml.XML

/**
 * Object which contains methods for Wikipedia dump file parsing and saving each page to different xml file
 */
object WikipediaDumpParser {

  /**
   * Main method for Wikipedia dump parsing
   * @param inputFilePath - initial dump file location
   * @param outputLocationPath - output files location
   */
  def parseDump(inputFilePath: String, outputLocationPath: String): Unit = {
    val xml = new XMLEventReader(Source.fromFile(inputFilePath))

    var insidePage = false
    var buf = ArrayBuffer[String]()
    for (event <- xml) {
      event match {
        case EvElemStart(_, "page", _, _) =>
          insidePage = true
          val tag = "<page>"
          buf += tag

        case EvElemEnd(_, "page") =>
          val tag = "</page>"
          buf += tag
          insidePage = false
          writePage(new File(outputLocationPath), buf)
          buf.clear

        case e @ EvElemStart(_, tag, _, _) =>
          if (insidePage) {
            buf += ("<" + tag + ">")
          }

        case e @ EvElemEnd(_, tag) =>
          if (insidePage) {
            buf += ("</" + tag + ">")
          }

        case EvText(t) =>
          if (insidePage) {
            buf += t
          }

        case _ => // ignore
      }
    }
  }

  /**
   * Method to save page content to xml file
   * @param rootOutputLocation - root location for output files
   * @param buf - xml file content
   */
  private[wiki] def writePage(rootOutputLocation: File, buf: ArrayBuffer[String]): Unit = {
    val s = buf.mkString
    val x = XML.loadString(s)
    val pageId = (x \ "id").head.child.head.toString
    val f = new File(rootOutputLocation, pageId + ".xml")
    val out = new FileOutputStream(f)
    out.write(s.getBytes())
    out.close()
  }

}
