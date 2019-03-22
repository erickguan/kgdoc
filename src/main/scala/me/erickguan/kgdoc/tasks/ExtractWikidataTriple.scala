package me.erickguan.kgdoc.tasks

import com.spotify.scio._
import com.spotify.scio.extra.checkpoint._
import me.erickguan.kgdoc.extractors.WikidataExtractor
import me.erickguan.kgdoc.filters.WikidataFilter

/* Usage:
   `sbt "runMain me.erickguan.kgdoc.tasks.ExtractWikidataTriple
    --checkpoint=/data/wikidata/triple_chk"
    --input=/data/wikidata
    --output=/data/wikidata/triple"`
 */
object ExtractWikidataTriple {
  def main(cmdlineArgs: Array[String]): Unit = {
    import me.erickguan.kgdoc.extractors.Triple

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Wikidata JSON dump keeps every records in a seperate line
    val items = sc.checkpoint(args("checkpoint") + "-items") {
      import me.erickguan.kgdoc.processors.WikidataJsonDumpLineProcessor

      sc.textFile(args("input"))
        .filter(WikidataJsonDumpLineProcessor.filterNonItem)
        .map(WikidataJsonDumpLineProcessor.decodeJsonLine)
    }

    val classes = sc.checkpoint(args("checkpoint") + "-classes") {
      items.filter(
        WikidataFilter.bySubclass(WikidataFilter.SubclassPropertyId, _))
    }

    // wikicite data are too many now. we don't need it now
    val bibliographicClassesSideInput = classes
      .filter(WikidataFilter.byBibliographicClass)
      .map(_.id)
      .asIterableSideInput

    val facts = items
      .withSideInputs(bibliographicClassesSideInput)
      .filter { (item, ctx) =>
        val excludeClasses = ctx(bibliographicClassesSideInput)
        !WikidataFilter.byInstanceOfEntities(excludeClasses, item)
      }
      .toSCollection
      .flatMap { l =>
        WikidataExtractor
          .triples(l)
          .map(Triple.repr(_))
      }
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
