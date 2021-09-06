package spark.montecarlopi

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MonteCarloPISpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  "MonteCarloPI.estimatePI function" should "return approximate PI value with precision +- 0.01 after 1000000 estimations" in {
    Given("Configured SparkContext and number of estimations")
    val appName: String = "PI Monte Carlo"
    val master: String = "local[*]"
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val numberOfTries = 1000000

    When("estimatePI is called with given parameters")
    val res = MonteCarloPI.estimatePI(sc, numberOfTries)
    sc.stop()

    Then("result should be approximately equal to PI")
    res should be (Math.PI +- 0.01)
  }


  "MonteCarloPI.isInsideCircle function" should "return 1 if sum of input values' squares less then 1" in {
    Given("Input values 0.5 and 0.4")
    val x: Double = 0.5
    val y: Double = 0.4

    When("isInsideCircle function is called for these values")
    val res: Double = MonteCarloPI.checkIsPointInCircle(x, y)

    Then("result of function execution should be 1")
    res should equal(1)
  }

  it should "return 0 if sum of input values' squares more then 1" in {
    Given("Input values 0.8 and 0.9")
    val x: Double = 0.8
    val y: Double = 0.9

    When("isInsideCircle function is called for these values")
    val res: Double = MonteCarloPI.checkIsPointInCircle(x, y)

    Then("result of function execution should be 0")
    res should equal(0.0)
  }

}
