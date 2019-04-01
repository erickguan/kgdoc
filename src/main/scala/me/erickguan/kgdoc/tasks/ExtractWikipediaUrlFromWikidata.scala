package me.erickguan.kgdoc.tasks

import com.spotify.scio._
import me.erickguan.kgdoc.extractors.{WikiSiteLink, WikidataExtractor}
import me.erickguan.kgdoc.filters.WikidataSiteFilter

/* Usage:
   `sbt "runMain me.erickguan.kgdoc.tasks.ExtractWikipediaUrlFromWikidata
    --runner=DirectRunner
    --input=/data/wikidata/wikidata-dump-*.json
    --accepted_language=en,zh,sv
    --dataset=/data/wikidata/dataset
    --checkpoint=/data/wikidata/triple_chk
    --output=/data/wikidata/description"`
 */
object ExtractWikipediaUrlFromWikidata {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val h = new TaskHelpers(sc)

    val items = h.extractItems(args("input"))
    val classes = h.extractClasses(items, args("checkpoint") + "-classes")
    val triples =
      h.triplesFromDataset(args("dataset"), args("checkpoint") + "-dataset")
    val (entitiesSide, relationsSide) = h.entityAndRelationSideSet(triples)

    val bc = h.bibliographicClassesSideInput(classes)
    val noBibs = h.filteredBibliographicClasses(items, bc)
    val ds = h.filteredDataset(noBibs, entitiesSide.side, relationsSide.side)
    val languages: Set[String] = args("accepted_language").split(',').toSet
    ds.flatMap { l =>
        val links = WikidataExtractor
          .sitelinks(l)
        WikidataSiteFilter
          .byLanguages(links, languages)
          .flatMap(WikiSiteLink.repr(_))
      }
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
