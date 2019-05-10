package me.erickguan.kgdoc.tasks

import java.io.{FileInputStream, InputStream}

import com.spotify.scio._
import com.spotify.scio.values.{SCollection, SideSet}
import me.erickguan.kgdoc.extractors.{ItemLangLiteral, WikidataExtractor}
import me.erickguan.kgdoc.filters.WikidataSiteFilter
import me.erickguan.kgdoc.json.WikidataItem

/*
 * better to use for Text literals
  Usage:
   `SBT_OPTS="-Xms1G -Xmx8G -Xss2M" sbt "runMain me.erickguan.kgdoc.tasks.ExtractSelectedWikidataTripleLabel
    --runner=SparkRunner
    --dataset=/data/wikidata/dataset/
    --input=/data/wikidata/prepared_dump/part-*
    --output=/data/wikidata/dataset/labels/
    --accepted_language=en,zh-hans,zh-cn,zh-hant,zh-tw,sv

   SBT_OPTS="-Xms1G -Xmx4G -Xss4M" sbt "runMain me.erickguan.kgdoc.tasks.ExtractSelectedWikidataTripleLabel
    --runner=DataflowRunner
    --project=data-hub-fantasticfears
    --zone=europe-north1-a
    --dataset=gs://wikidata-research-fantasticfears/dataset/
    --input=gs://wikidata-research-fantasticfears/wikidata-description.txt
    --output=gs://wikidata-research-fantasticfears/description/
    --accepted_language=en,zh-hans,zh-cn,zh-hant,zh-tw,sv"
 */
object ExtractSelectedWikidataTripleLabel {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val h = new TaskHelpers(sc)

    val entities =
      h.entitiesFromPlaintextTranslation(args("dataset"))
    val relations =
      h.relationsFromPlaintextTranslation(args("dataset"))
    val languages: Set[String] = args.list("accepted_language").toSet

    val ds = sc.textFile(args("input"))
      .withSideInputs(entities, relations)
      .filter { (l, ctx) =>
        val ents = ctx(entities)
        val rels = ctx(relations)
        val t = l.substring(0, l.indexOf('\t'))
        ents.isDefinedAt(t) || rels.isDefinedAt(t)
      }
      .toSCollection
      .map { l =>
        WikidataItem.decodeJson(l.substring(l.indexOf('\t') + 1))
      }
      .flatMap { l =>
        val literals = WikidataExtractor
          .labelLiterals(l)
        WikidataSiteFilter
          .literalByLanguages(literals, languages)
          .map(ItemLangLiteral.repr(_))
      }
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
