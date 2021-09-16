package spark.wiki

import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WikipediaDumpAnalyzerSpec extends AnyFlatSpec with Matchers with GivenWhenThen{

  "WikipediaDumpAnalyzer.getTagContentFromXML function" should "take xml string and tag name as input and return content of tag from this string" in {
    Given("input string and tag name")
    val xmlInput =
      "<test>" +
        "not required text" +
        "<test2>" +
          "required text" +
        "</test2>" +
      "</test>"
    val tagName = "test2"

    When("getTagContentFromXML function is executed with given input parameters")
    var result = WikipediaDumpAnalyzer.getTagContentFromXML(xmlInput, tagName)

    Then("result should be content of input tag")
    result should equal("required text")
  }

  it should "take xml string and tag name as input and return empty string if provided tag is not found in input string" in {
    Given("input string and tag name")
    val xmlInput =
      "<test>" +
        "not required text" +
        "<test2>" +
        "required text" +
        "</test2>" +
        "</test>"
    val tagName = "test3"

    When("getTagContentFromXML function is executed with given input parameters")
    var result = WikipediaDumpAnalyzer.getTagContentFromXML(xmlInput, tagName)

    Then("result should be empty string")
    result should equal("")
  }

  "WikipediaDumpAnalyzer.combineTwoMaps function" should "take 2 Map[String, Int] as parameters and return one merged Map[String, Int]" in {
    Given("two maps with type Map[String, Int]")
    val map1 = Map("test" -> 1, "test2" -> 1)
    val map2 = Map("test3" -> 3, "test" -> 2)

    When("combineTwoMaps function is called for input maps")
    val result = WikipediaDumpAnalyzer.combineTwoMaps(map1, map2)

    Then("result map should have all keys from input maps. Values of same keys should be summarized")
    result should equal(Map("test" -> 3, "test2" -> 1, "test3" -> 3))
  }

  "WikipediaDumpAnalyzer.getWordsHeatMap" should "take string as input and return Map[String, Int] with words heat map" in {
    Given("input string with some text")
    val input = "test in test2, with test in test3 on test"

    When("getWordsHeatMap function is called for input string")
    val result = WikipediaDumpAnalyzer.getWordsHeatMap(input)

    Then("result should be Map[String, Int] with words heat map of input string")
    result should equal(Map("test" -> 3, "in" -> 2, "test2" -> 1, "test3" -> 1, "on" -> 1, "with" -> 1))
  }
}
