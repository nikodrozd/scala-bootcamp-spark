package spark.montecarlopi

import org.apache.spark.SparkContext

import scala.util.Random


object MonteCarloPI extends App {

  def estimatePI(sc: SparkContext, numberOfTries: Int): Double = {
    4.0 * sc.parallelize(0 to numberOfTries).map(_ => checkIsPointInCircle(Random.nextDouble, Random.nextDouble)).sum / numberOfTries
  }

  private [montecarlopi] def checkIsPointInCircle(x: Double, y: Double): Double = {
    if (x * x + y * y < 1) 1.0
    else 0.0
  }
}
