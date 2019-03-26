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
  def deficitRelation(threshold: Double): Boolean = {
    LatentFeatureModelDatasetFilter.assertThreshold(threshold)

    triple._2

    counter = defaultdict(list)
    for t in triples:
      counter[t.relation]
    .append(t)

    num_triples = len(triples)
    removal_set = set()
    for rel
    , rel_items in counter.items():
    if len(rel_items) / num_triples < threshold:
      removal_set
    = removal_set | set(rel_items)

    return list(filterfalse(lambda x: x in removal_set, triples))
  }
}