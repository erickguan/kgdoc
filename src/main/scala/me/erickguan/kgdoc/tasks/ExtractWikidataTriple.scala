package me.erickguan.kgdoc.tasks

import com.spotify.scio.extra.json._
import com.spotify.scio.{io => scioIo, _}
import _root_.io.circe.{Decoder, Json}

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

    // Open text files a `SCollection[String]`
    val work = sc.jsonFile[WikidataItem](args("input"))
      .saveAsJsonFile("/tmp/local_wordcount")
    //    getOrElse("input", ExampleData.EXPORTED_WIKI_TABLE))
//      .take(100)
//      // Save result as text files under the output path
//      .saveAsTableRowJsonFile(args("output"))
    sc.close()

    work.waitForResult().value.take(3).foreach(println)
  }
}
