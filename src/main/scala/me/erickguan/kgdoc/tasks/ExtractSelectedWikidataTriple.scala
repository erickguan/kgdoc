package me.erickguan.kgdoc.tasks

import com.spotify.scio._
import me.erickguan.kgdoc.extractors.WikidataExtractor
import org.slf4j.LoggerFactory

/* Usage:
   `SBT_OPTS="-Xms1G -Xmx4G -Xss2M" sbt "runMain me.erickguan.kgdoc.tasks.ExtractSelectedWikidataTriple
    --runner=SparkRunner
    --checkpoint=/data/wikidata/triple_chk
    --input=/data/wikidata/wikidata-*.txt
    --selected=/data/wikidata/movie_dataset/selected_triples.txt
    --output=/data/wikidata/movie_dataset"`
 */
object ExtractSelectedWikidataTriple {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val numItems = ScioMetrics.counter("numItems")
  private val numBibliographicItems = ScioMetrics.counter("numBibliographicItems")
  private val numTriples = ScioMetrics.counter("numTriples")

  def main(cmdlineArgs: Array[String]): Unit = {
    import me.erickguan.kgdoc.extractors.Triple

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val h = new TaskHelpers(sc)

    val items = h.extractItems(args("input"), numItems)
    val classes = h.extractClasses(items, args("checkpoint") + "-classes")
    val bc = h.bibliographicClassesSideInput(classes)
    val selectedEntities = sc.textFile(args("selected"))
      .toSideSet

    val facts = h.filteredBibliographicClasses(items, bc, numBibliographicItems)
      .flatMap { l =>
        WikidataExtractor
          .triples(l)
      }
      .withSideInputs(selectedEntities.side)
      .filter { (triple, ctx) =>
        val selected = ctx(selectedEntities.side)
        selected(triple.subject) || selected(triple.`object`)
      }
      .toSCollection
      .map { t =>
        numTriples.inc()
        Triple.repr(t)
      }
      .saveAsTextFile(args("output"))

    val result = sc.close().waitUntilFinish()

    logger.info("Number of items: " + result.counter(numItems).committed)
    logger.info("Number of bibliographic items: " + result.counter(numBibliographicItems).committed)
    logger.info("Number of triples: " + result.counter(numTriples).committed)
  }
}
