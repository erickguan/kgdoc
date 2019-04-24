package me.erickguan.kgdoc.tasks

import java.io.{FileInputStream, InputStream}

import com.spotify.scio._
import me.erickguan.kgdoc.pb.tripleindex.Translation

/*
 * better to use for Text literals
  Usage:
   `SBT_OPTS="-Xms1G -Xmx40G -Xss2M" sbt "runMain me.erickguan.kgdoc.tasks.MatchTripleBinaryLiterals
    --runner=SparkRunner
    --dataset=/data/wikidata/dataset
    --input=/data/wikidata/wikidata-labels.txt
    --output=/data/wikidata/dataset/labels.txt
    --accepted_language=en,zh-hans,zh-cn,zh-hant,zh-tw,sv
 */
object MatchTripleBinaryLiterals {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val h = new TaskHelpers(sc)

    val (entities, relations) = h.entityAndRelationSideSetFromTranslation(
      Translation.parseFrom(
        new FileInputStream(args("dataset") + "translation.protobuf")))
    val acceptedLanguage = args("accepted_language").toSet

    sc.textFile(args("input"))
      .map(_.split('\t'))
      .withSideInputs(entities.side)
      .filter { (l, ctx) =>
        val ents = ctx(entities.side)

        ents(l.head) && l.tail
          .mkString("\t")
          .split('@')
          .tail
          .exists(lang => acceptedLanguage(lang.toLowerCase))
      }
      .toSCollection
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
