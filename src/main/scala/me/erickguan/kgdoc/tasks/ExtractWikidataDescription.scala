package me.erickguan.kgdoc.tasks

import com.spotify.scio._
import me.erickguan.kgdoc.extractors.WikidataExtractor
import me.erickguan.kgdoc.filters.WikidataSiteFilter

/* Usage:
   `sbt "runMain me.erickguan.kgdoc.tasks.ExtractWikidataDescription
    --runner=SparkRunner
    --input=/data/wikidata/wikidata-dump-*.json.bz2
    --dataset=/data/wikidata/dataset
    --checkpoint=/data/wikidata/triple_chk
    --output=/data/wikidata/description"`
 */
object ExtractWikidataDescription {
  def main(cmdlineArgs: Array[String]): Unit = {
    import me.erickguan.kgdoc.extractors.ItemLangLiteral

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val h = new TaskHelpers(sc)

    val items = h.extractItems(args("input"))
    val classes = h.extractClasses(items, args("checkpoint") + "-classes")
    val triples =
      h.triplesFromDataset(args("dataset"), args("checkpoint") + "-dataset")
    val (entitiesSide, relationsSide) = h.entityAndRelationSideSet(triples)
    val languages: Set[String] = args.list("accepted_language").toSet

    val bc = h.bibliographicClassesSideInput(classes)
    val lang = h.filteredBibliographicClasses(items, bc)
    h.filteredDataset(lang, entitiesSide.side, relationsSide.side)
      .flatMap { l =>
        val literals = WikidataExtractor
          .descriptions(l)
        WikidataSiteFilter
          .literalByLanguages(literals, languages)
          .map(ItemLangLiteral.repr(_))
      }
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
