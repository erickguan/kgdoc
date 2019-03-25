package me.erickguan.kgdoc.tasks

import com.spotify.scio._
import me.erickguan.kgdoc.extractors.WikidataExtractor

/* Usage:
   `sbt "runMain me.erickguan.kgdoc.tasks.ExtractWikipediaUrlFromWikidata
    --runner=SparkRunner
    --input=/data/wikidata/wikidata-dump-*.json.bz2
    --dataset=/data/wikidata/dataset
    --checkpoint=/data/wikidata/triple_chk
    --output=/data/wikidata/description"`
 */
object ExtractWikipediaUrlFromWikidata {
  def main(cmdlineArgs: Array[String]): Unit = {
    import me.erickguan.kgdoc.extractors.ItemLangLiteral

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val h = new TaskHelpers(sc)

    val items = h.extractItems(args("input"))
    val classes = h.extractClasses(items, args("checkpoint") + "-classes")
    val triples =
      h.triplesFromDataset(args("dataset"), args("checkpoint") + "-dataset")
    val (entitiesSide, relationsSide) = h.entityAndRelationSideSet(triples)

    val bc = h.bibliographicClassesSideInput(classes)
    val lang = h.filteredBibliographicClasses(items, bc)
      .flatMap { l =>
        WikidataExtractor
          .sitelinks(l)
      }
    h.filteredDataset(lang, entitiesSide.side, relationsSide.side)
      .map(ItemLangLiteral.repr(_))
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
