package me.erickguan.kgdoc.tasks

import com.spotify.scio.ContextAndArgs
import me.erickguan.kgdoc.filters.WikidataItemFilter
import me.erickguan.kgdoc.processors.WikidataJsonDumpLineProcessor

/*
  Usage:
   `SBT_OPTS="-Xms1G -Xmx8G -Xss2M" sbt "runMain me.erickguan.kgdoc.tasks.PrepareWikidataDump
    --runner=SparkRunner
    --checkpoint=/data/wikidata/triple_chk
    --input=/data/wikidata/wikidata-dump-*.json
    --output=/data/wikidata/prepared_dump
 */
object PrepareWikidataDump {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val h = new TaskHelpers(sc)

    val items = h.extractItems(args("input"))
    val classes = h.extractClasses(items, args("checkpoint") + "-classes")
    val bc = h.bibliographicClassesSideInput(classes)

    sc.textFile(args("input"))
      .filter(WikidataJsonDumpLineProcessor.filterNonItem)
      .map { l =>
        (l, WikidataJsonDumpLineProcessor.decodeJsonLine(l))
      }
      .withSideInputs(bc)
      .filter { (t, ctx) =>
        val excludeClasses = ctx(bc)
        !new WikidataItemFilter(t._2).byInstanceOfEntities(excludeClasses)
      }
      .toSCollection
      .map { t =>
        s"${t._2.id}\t${t._1}"
      }
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
