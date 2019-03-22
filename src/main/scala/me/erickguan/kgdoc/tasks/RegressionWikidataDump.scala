package me.erickguan.kgdoc.tasks

import com.spotify.scio._
import com.spotify.scio.extra.checkpoint._
import me.erickguan.kgdoc.extractors.WikidataExtractor
import me.erickguan.kgdoc.filters.WikidataFilter

/* Usage:
   `SBT_OPTS="-Xms1G -Xmx90G -Xss2M" sbt "runMain me.erickguan.kgdoc.tasks.RegressionWikidataDump
    --runner=DirectRunner
    --input=/data/wikidata/wikidata-*-all.json"`
 */
object RegressionWikidataDump {
  def main(cmdlineArgs: Array[String]): Unit = {
    import me.erickguan.kgdoc.processors.WikidataJsonDumpLineProcessor

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Wikidata JSON dump keeps every records in a seperate line
    val items = sc
      .textFile(args("input"))
      .filter(WikidataJsonDumpLineProcessor.filterNonItem)
      .map(WikidataJsonDumpLineProcessor.decodeJsonLine(_).id)
      .debug()

    sc.close()
  }
}
