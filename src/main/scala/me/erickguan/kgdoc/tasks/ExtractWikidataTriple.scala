package me.erickguan.kgdoc.tasks

import com.spotify.scio.extra.json._
import com.spotify.scio.{io => scioIo, _}


/* Usage:
   `sbt "runMain me.erickguan.kgdoc.tasks.ExtractWikidataTriple
    --project=[PROJECT] --runner=DirectRunner
    --input=samples/-*.json
    --output=/tmp/wikidata"`
 */
object ExtractWikidataTriple {
  import me.erickguan.kgdoc.json._

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Wikidata JSON dump keeps every records in a seperate line
    val work = sc.textFile(args("input"))
      .filter(s => s != "[" && s != "]")
      .map(_.stripSuffix(","))
      .debug(prefix = "===== DEBUG =====")
      .map(WikidataItem.decodeJson)
      .saveAsTextFile("/tmp/wikidata")

    sc.close()

    work.waitForResult().value.take(3).foreach(println)
  }
}
