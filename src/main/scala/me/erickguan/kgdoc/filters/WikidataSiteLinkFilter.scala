package me.erickguan.kgdoc.filters

import com.github.tototoshi.csv.CSVReader
import me.erickguan.kgdoc.extractors.WikiSiteLink
import me.erickguan.kgdoc.json.SiteLink

import scala.io.Source

object WikidataSiteLinkFilter {
  // language in Wikidata is different than site identifier
  private def loadWikiSitesCSV(): Map[String, Iterable[Map[String, String]]] = {
    val source = Source.fromURL(
      getClass.getResource("/generated/wikisites.csv"))
    CSVReader.open(source).allWithHeaders.groupBy(f => f("lang"))
  }
  lazy val wikiSites: Map[String, Iterable[Map[String, String]]]  = loadWikiSitesCSV()

  def langToSites(lang: String, code: String): Iterable[String] = {
    try {
      wikiSites(lang).filter(_("code") == code).map(_("dbname"))
    } catch {
      List()
    }
  }

  def bySite(items: Iterable[WikiSiteLink], acceptedSite: Set[String]): Iterable[WikiSiteLink] = {
    items.filter(i => acceptedSite(i.site))
  }

  def byLanguages(items: Iterable[WikiSiteLink], acceptedLanguages: Set[String]): Iterable[WikiSiteLink] = {
    bySite(items, acceptedLanguages.flatMap(langToSites(_, "wikipedia")))
  }
}

