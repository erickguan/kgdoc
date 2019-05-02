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
      .withSideInputs(entities.side)
      .filter { (l, ctx) =>
        val t = l.split('\t')
        val ents = ctx(entities.side)

        ents(t.head) && l
          .split('@')
          .tail
          .exists(lang => acceptedLanguage(lang.toLowerCase))
      }
      .toSCollection
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
