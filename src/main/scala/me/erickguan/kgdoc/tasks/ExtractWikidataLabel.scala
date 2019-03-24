package me.erickguan.kgdoc.tasks

import com.spotify.scio._
import me.erickguan.kgdoc.extractors.WikidataExtractor

/* Usage:
   `sbt "runMain me.erickguan.kgdoc.tasks.ExtractWikidataLabel
    --runner=SparkRunner
    --input=/data/wikidata/wikidata-dump-*.json.bz2
    --checkpoint=/data/wikidata/triple_chk
    --output=/data/wikidata/label"`
 */
object ExtractWikidataLabel {
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
    h.filteredBibliographicClasses(items, bc)
      .flatMap { l =>
        WikidataExtractor
          .labelLiterals(l)
      }
      .withSideInputs(entitiesSide.side, relationsSide.side)
      .filter { (l, ctx) =>
        val entities = ctx(entitiesSide.side)
        val relations = ctx(relationsSide.side)
        entities(l.item) || relations(l.item)
      }
      .toSCollection
      .map(ItemLangLiteral.repr(_))
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
