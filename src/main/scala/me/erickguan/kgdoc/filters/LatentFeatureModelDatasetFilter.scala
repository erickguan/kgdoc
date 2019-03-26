package me.erickguan.kgdoc.filters

import com.spotify.scio.values.SCollection

object LatentFeatureModelDatasetFilter {
  def relation(triple: (String, String, String)): String = triple._2

  def applyThresholdOnRelation(relationPair: (String, Long), threshold: Long): Boolean = relationPair._2 > threshold

  private def assertThreshold(threshold: Double): Unit = {
    if (threshold <= 0.0 || threshold > 1.0) {
      throw new RuntimeException(s"The threshold $threshold is not in the valid range of (0, 1]")
    }
  }
}

class LatentFeatureModelDatasetFilter(triple: (String, String, String)) {
//  def deficitRelation(threshold: Double): Boolean = {
//    LatentFeatureModelDatasetFilter.assertThreshold(threshold)
//
//  }
}