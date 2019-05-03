package me.erickguan.kgdoc.tasks

import java.io.{FileInputStream, InputStream}

import com.spotify.scio._
import com.spotify.scio.values.{SCollection, SideSet}

/*
 * better to use for Text literals
  Usage:
   `SBT_OPTS="-Xms1G -Xmx8G -Xss2M" sbt "runMain me.erickguan.kgdoc.tasks.MatchTripleBinaryLiterals
    --runner=SparkRunner
    --dataset=/data/wikidata/dataset
    --input=/data/wikidata/wikidata-labels.txt
    --output=/data/wikidata/dataset/labels.txt
    --accepted_language=en,zh-hans,zh-cn,zh-hant,zh-tw,sv

   SBT_OPTS="-Xms1G -Xmx4G -Xss4M" sbt "runMain me.erickguan.kgdoc.tasks.MatchTripleBinaryLiterals
    --runner=DataflowRunner
    --project=data-hub-fantasticfears
    --zone=europe-north1-a
    --dataset=gs://wikidata-research-fantasticfears/dataset/
    --input=gs://wikidata-research-fantasticfears/wikidata-description.txt
    --output=gs://wikidata-research-fantasticfears/description/
    --accepted_language=en,zh-hans,zh-cn,zh-hant,zh-tw,sv"
 */
object MatchTripleBinaryLiterals {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val h = new TaskHelpers(sc)

    val entities =
      h.entitiesFromPlaintextTranslation(args("dataset"))
    val acceptedLanguage = args.list("accepted_language").toSet

    sc.textFile(args("input"))
      .map { l =>
        val spans = l.split('\t')
        (spans(0), spans(1))
      }
      .withSideInputs(entities)
      .filter { (spans, ctx) =>
        val ents: Map[String, Long] = ctx(entities)
        val ent = spans._1
        val rest = spans._2

        ents.isDefinedAt(ent) && // test entity exists
        acceptedLanguage(
          rest
            .slice(rest.lastIndexOf('@') + 1, rest.length)
            .toLowerCase)
      }
      .map { (spans, ctx) =>
        val ents = ctx(entities)
        val rest = spans._2
        s"${ents(spans._1)}\t${rest.patch(rest.lastIndexOf('@'), "\t", 1)}"
      }
      .toSCollection
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
