package me.erickguan.kgdoc.tasks

import com.spotify.scio.extra.json._
import com.spotify.scio.{io => scioIo, _}

//{"type":"item","aliases":{},"labels":{},"descriptions":{},"sitelinks":{},"id":"Q1","claims":{"P31":[{"rank":"normal","mainsnak":{"snaktype":"value","property":"P31","datavalue":{"type":"wikibase-entityid","value":{"entity-type":"item","numeric-id":1454986}},"datatype":"wikibase-item"},"id":"q1$0479EB23-FC5B-4EEC-9529-CEE21D6C6FA9","type":"statement"}]}},
case class Item(type: String, aliases: Map)

/* Usage:
   `sbt "runMain me.erickguan.kgdoc.tasks.ExtractWikidataTriple
    --project=[PROJECT] --runner=DirectRunner
    --input=samples/-*.json
    --output=/tmp/wikidata"`
 */
object ExtractWikidataTriple {
//  def decodeJson(rawJson: String): Json = {
//    import io.circe.parser._
//    parse(rawJson) match {
//      case Left(failure) => Json.Null
//      case Right(json) => json
//    }
//  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Open text files a `SCollection[String]`
    val work = sc.jsonFile[Record](args("input"))
      .saveAsJsonFile("/tmp/local_wordcount")
    //    getOrElse("input", ExampleData.EXPORTED_WIKI_TABLE))
//      .take(100)
//      // Save result as text files under the output path
//      .saveAsTableRowJsonFile(args("output"))
    sc.close()

    work.waitForResult().value.take(3).foreach(println)
  }
}
