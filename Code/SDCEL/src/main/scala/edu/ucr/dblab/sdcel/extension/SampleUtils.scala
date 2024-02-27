package edu.ucr.dblab.sdcel.extension

object SampleUtils {
  def getSampleNumbers(numPartitions: Int, totalNumberOfRecords: Long, givenSampleNumbers: Int = -1): Int = {
    if (givenSampleNumbers > 0) {
      if (givenSampleNumbers > totalNumberOfRecords) throw new IllegalArgumentException("[GeoSpark] Number of samples " + givenSampleNumbers + " cannot be larger than total records num " + totalNumberOfRecords)
      return givenSampleNumbers
    }
    // Make sure that number of records >= 2 * number of partitions
    if (totalNumberOfRecords < 2L * numPartitions) throw new IllegalArgumentException("[GeoSpark] Number of partitions " + numPartitions + " cannot be larger than half of total records num " + totalNumberOfRecords)
    if (totalNumberOfRecords < 1000) return totalNumberOfRecords.toInt
    val minSampleCnt = numPartitions * 2
    Math.max(minSampleCnt, Math.min(totalNumberOfRecords / 100, Integer.MAX_VALUE)).toInt
  }

  def getFraction(numPartitions: Int, totalNumberOfRecords: Long): Double = {
    val sampleNumberOfRecords = getSampleNumbers(numPartitions, totalNumberOfRecords)
    val fraction = computeFractionForSampleSize(sampleNumberOfRecords, totalNumberOfRecords, withReplacement = false)
    fraction
  }

  def computeFractionForSampleSize(sampleSizeLowerBound: Int, total: Long, withReplacement: Boolean): Double = {
    if (withReplacement) {
      PoissonBounds.getUpperBound(sampleSizeLowerBound) / total
    } else {
      val fraction = sampleSizeLowerBound.toDouble / total
      BinomialBounds.getUpperBound(1e-4, total, fraction)
    }
  }
}

object PoissonBounds {

  def getLowerBound(s: Double): Double = {
    math.max(s - numStd(s) * math.sqrt(s), 1e-15)
  }

  def getUpperBound(s: Double): Double = {
    math.max(s + numStd(s) * math.sqrt(s), 1e-10)
  }

  private def numStd(s: Double): Double = {
    // TODO: Make it tighter.
    if (s < 6.0) {
      12.0
    } else if (s < 16.0) {
      9.0
    } else {
      6.0
    }
  }
}

/**
 * Utility functions that help us determine bounds on adjusted sampling rate to guarantee exact
 * sample size with high confidence when sampling without replacement.
 */
object BinomialBounds {

  private val minSamplingRate = 1e-10

  /**
   * Returns a threshold `p` such that if we conduct n Bernoulli trials with success rate = `p`,
   * it is very unlikely to have more than `fraction * n` successes.
   */
  def getLowerBound(delta: Double, n: Long, fraction: Double): Double = {
    val gamma = - math.log(delta) / n * (2.0 / 3.0)
    fraction + gamma - math.sqrt(gamma * gamma + 3 * gamma * fraction)
  }

  /**
   * Returns a threshold `p` such that if we conduct n Bernoulli trials with success rate = `p`,
   * it is very unlikely to have less than `fraction * n` successes.
   */
  def getUpperBound(delta: Double, n: Long, fraction: Double): Double = {
    val gamma = - math.log(delta) / n
    math.min(1,
      math.max(minSamplingRate, fraction + gamma + math.sqrt(gamma * gamma + 2 * gamma * fraction)))
  }
}
