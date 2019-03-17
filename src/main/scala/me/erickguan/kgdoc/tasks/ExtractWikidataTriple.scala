package me.erickguan.kgdoc.tasks

import com.spotify.scio.extra.json._
import com.spotify.scio.{io => scioIo, _}
import _root_.io.circe.Decoder

//{"type":"item","aliases":{},"labels":{},"descriptions":{},"sitelinks":{},"id":"Q1","claims":{"P31":[{"rank":"normal","mainsnak":{"snaktype":"value","property":"P31","datavalue":{"type":"wikibase-entityid","value":{"entity-type":"item","numeric-id":1454986}},"datatype":"wikibase-item"},"id":"q1$0479EB23-FC5B-4EEC-9529-CEE21D6C6FA9","type":"statement"}]}},
//case class LangItem(language: String, value: String)
//case class SiteLinks(site: String, title: String)
//sealed trait TypedDataValue
//case class StringTypedDataValue(value: String) extends TypedDataValue
//case class GlobalCoordinateDataValue(latitude: String, longtitude: String, altitude: String, precision: String, globe: String) extends TypedDataValue
//case class QuantityDataValue(amount: String, upperBound: String, lowerBound: String, unit: String) extends TypedDataValue
//case class TimeDataValue(time: String, timezone: Long, before: Long, after: Long, precision: Long, calendarModel: String) extends TypedDataValue
//case class DataValue(`type`: String, value: TypedDataValue)
//object DataValue {
//  implicit val decodeVariable: Decoder[DataValue] = Decoder.instance(c => {
//    val dataType = c.downField("type").as[String]
//    val valueField = c.downField("value")
//    val value = dataType match {
//      case "string" => valueField.as[String]
//      // we don't use `wikibase-entityid`
//      // case "wikibase-entityid" => valueField.as[Map[String, String]]
//      case "globecoordinate" => valueField.as[Map[String, String]]
//      case "quantity" => valueField.as[Map[String, String]]
//      case "time" => valueField.as[Map[String, String]]
//    }
//    DataValue(dataType, value.right)
//  })
//}
//case class Snak(snaktype: String, property: String, datatype: String, datavalue: DataValue)
//case class Claim(`type`: String, mainsnak: Snak, rank: String, qualifiers: List[Snak])
//case class Item(id: String,
//                `type`: String,
//                labels: Map[String, LangItem],
//                descriptions: Map[String, LangItem],
//                aliases: Map[String, List[LangItem]],
////                claims:,
////                sitelinks: ,
//               )
//
///* Usage:
//   `sbt "runMain me.erickguan.kgdoc.tasks.ExtractWikidataTriple
//    --project=[PROJECT] --runner=DirectRunner
//    --input=samples/-*.json
//    --output=/tmp/wikidata"`
// */
//object ExtractWikidataTriple {
////  def decodeJson(rawJson: String): Json = {
////    import io.circe.parser._
////    parse(rawJson) match {
////      case Left(failure) => Json.Null
////      case Right(json) => json
////    }
////  }
//
//  def main(cmdlineArgs: Array[String]): Unit = {
//    val (sc, args) = ContextAndArgs(cmdlineArgs)
//
//    // Open text files a `SCollection[String]`
//    val work = sc.jsonFile[Record](args("input"))
//      .saveAsJsonFile("/tmp/local_wordcount")
//    //    getOrElse("input", ExampleData.EXPORTED_WIKI_TABLE))
////      .take(100)
////      // Save result as text files under the output path
////      .saveAsTableRowJsonFile(args("output"))
//    sc.close()
//
//    work.waitForResult().value.take(3).foreach(println)
//  }
//}
