package me.erickguan.kgdoc.tasks

import com.spotify.scio._
import me.erickguan.kgdoc.extractors.WikidataExtractor
import org.slf4j.LoggerFactory

/* Usage:
   `SBT_OPTS="-Xms1G -Xmx4G -Xss2M" sbt "runMain me.erickguan.kgdoc.tasks.ExtractWikidataTriple
    --runner=SparkRunner
    --checkpoint=/data/wikidata/triple_chk
    --input=/data/wikidata/wikidata-*-all.json
    --output=/data/wikidata/triple"`
 */
object ExtractWikidataTriple {
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

    val facts = h.filteredBibliographicClasses(items, bc, numBibliographicItems)
      .flatMap { l =>
        WikidataExtractor
          .triples(l)
          .map { t =>
            numTriples.inc()
            Triple.repr(t)
          }
      }
      .saveAsTextFile(args("output"))

    val result = sc.close().waitUntilFinish()

    logger.info("Number of items: " + result.counter(numItems).committed)
    logger.info("Number of bibliographic items: " + result.counter(numBibliographicItems).committed)
    logger.info("Number of triples: " + result.counter(numTriples).committed)
  }
}
