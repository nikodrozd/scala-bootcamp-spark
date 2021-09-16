package spark.wiki

object Main extends App {

  val startTime = System.currentTimeMillis()
  val inputFileLocation = "src/main/resources/enwiki-20210901/enwiki-20210901-pages-articles-multistream.xml"
  val parsedDumpFilesLocation = "src/main/resources/result"

  WikipediaDumpParser.parseDump(inputFileLocation, parsedDumpFilesLocation)
  val data = WikipediaDumpAnalyzer.getDataFromFiles(parsedDumpFilesLocation)

  val afterParseDump = System.currentTimeMillis()

  WikipediaDumpAnalyzer.getTop10BiggestPages(data).foreach(x => println(s"${x._1}"))

  val afterTop10BiggestPages = System.currentTimeMillis()

  WikipediaDumpAnalyzer.getTop10WordsHeatMap(data).foreach(println(_))

  val endTime = System.currentTimeMillis()

  println(s"Parse time: ${afterParseDump - startTime}")
  println(s"TOP 10 Biggest Pages: ${afterTop10BiggestPages - afterParseDump}")
  println(s"TOP 10 Words Heat Map: ${endTime - afterTop10BiggestPages}")
  println(s"Total time: ${endTime - startTime}")
}
