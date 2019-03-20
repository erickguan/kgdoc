package me.erickguan.kgdoc.tasks

import com.spotify.scio._
import me.erickguan.kgdoc.extractors.WikidataExtractor
import me.erickguan.kgdoc.processors.WikidataJsonDumpLineProcessor

/* Usage:
   `sbt "runMain me.erickguan.kgdoc.tasks.ExtractWikidataLabel
    --input=samples/wikidata-dump-*.json
    --output=/tmp/wikidata"`
 */
object ExtractWikidataLabel {
  def main(cmdlineArgs: Array[String]): Unit = {
    import me.erickguan.kgdoc.extractors.ItemLangLiteral

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Wikidata JSON dump keeps every records in a seperate line
    val work = sc
      .textFile(args("input"))
      .filter(WikidataJsonDumpLineProcessor.filterNonItem)
      .flatMap(
        l =>
          WikidataExtractor
            .labelLiterals(WikidataJsonDumpLineProcessor.decodeJsonLine(l))
            .map(ItemLangLiteral.repr(_)))
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
