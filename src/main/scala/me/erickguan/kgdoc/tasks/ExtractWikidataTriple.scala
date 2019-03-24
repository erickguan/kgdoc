package me.erickguan.kgdoc.tasks

import com.spotify.scio._
import me.erickguan.kgdoc.extractors.WikidataExtractor

/* Usage:
   `SBT_OPTS="-Xms1G -Xmx4G -Xss2M" sbt "runMain me.erickguan.kgdoc.tasks.ExtractWikidataTriple
    --runner=SparkRunner
    --checkpoint=/data/wikidata/triple_chk
    --input=/data/wikidata/wikidata-*-all.json
    --output=/data/wikidata/triple"`
 */
object ExtractWikidataTriple {
  def main(cmdlineArgs: Array[String]): Unit = {
    import me.erickguan.kgdoc.extractors.Triple

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val h = new TaskHelpers(sc)

    val items = h.extractItems(args("input"))
    val classes = h.extractClasses(items, args("checkpoint") + "-classes")
    val bc = h.bibliographicClassesSideInput(classes)

    val facts = h.filteredBibliographicClasses(items, bc)
      .flatMap { l =>
        WikidataExtractor
          .triples(l)
          .map(Triple.repr(_))
      }
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
