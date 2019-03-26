package me.erickguan.kgdoc.tasks

import com.spotify.scio._
import me.erickguan.kgdoc.extractors.WikidataExtractor
import me.erickguan.kgdoc.filters.LatentFeatureModelDatasetFilter
import me.erickguan.kgdoc.processors.DatasetLineProcessor

/**
 * Cleansing data is important. We remove relations and insufficient things accordingly
    Usage:
   `sbt "runMain me.erickguan.kgdoc.tasks.BuildLatentFeatureModelDataset
    --runner=SparkRunner
    --input=/data/wikidata/wikidata-triples
    --separator='\t'
    --relation_threshold=1000
    --duplicate_threshold=0.97
    --output=/data/wikidata/dataset"`
 */
object BuildLatentFeatureModelDataset {
  def main(cmdlineArgs: Array[String]): Unit = {
    import com.spotify.scio.values.SideSet

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val h = new TaskHelpers(sc)
    val separator = args("separator").toString.head

    val in = sc.textFile(args("input"))
        .map(DatasetLineProcessor.decodeLine(_, separator))
    val tripleCount = in.count
    val relationThreshold = args("relation_threshold").toLong

    val filtered1 = h.removeDeficitRelation(in, relationThreshold)
    val duplicateRelationThreshold = args("duplicate_threshold").toDouble

//
//    val h = new TaskHelpers(sc)
//
//    val items = h.extractItems(args("input"))
//    val classes = h.extractClasses(items, args("checkpoint") + "-classes")
//    val triples =
//      h.triplesFromDataset(args("dataset"), args("checkpoint") + "-dataset")
//    val (entitiesSide, relationsSide) = h.entityAndRelationSideSet(triples)
//
//    val bc = h.bibliographicClassesSideInput(classes)
//    val lang = h.filteredBibliographicClasses(items, bc)
//      .flatMap { l =>
//        WikidataExtractor
//          .descriptions(l)
//      }
//    h.filteredDataset(lang, entitiesSide.side, relationsSide.side)
//      .map(ItemLangLiteral.repr(_))
//      .saveAsTextFile(args("output"))

    sc.close()
  }
}
