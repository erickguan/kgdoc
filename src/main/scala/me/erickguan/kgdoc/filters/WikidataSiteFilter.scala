package me.erickguan.kgdoc.filters

import com.github.tototoshi.csv.CSVReader
import com.ibm.icu.util.ULocale
import me.erickguan.kgdoc.extractors.{ItemLangLiteral, WikiSiteLink}
import me.erickguan.kgdoc.json.SiteLink

import scala.io.Source

object WikidataSiteFilter {
  // language in Wikidata is different than site identifier
  private def loadWikiSitesCSV(): Map[String, Iterable[Map[String, String]]] = {
    val source =
      Source.fromURL(getClass.getResource("/generated/wikisites.csv"))
    CSVReader.open(source).allWithHeaders.groupBy(f => f("lang"))
  }
  lazy val wikiSites: Map[String, Iterable[Map[String, String]]] =
    loadWikiSitesCSV()

  def langToSites(lang: String, code: String): Iterable[String] = {
    try {
      wikiSites(lang).filter(_("code") == code).map(_("dbname"))
    } catch {
      case _: Throwable => List()
    }
  }

  private def filterLocale(lang: String,
                           acceptedLanguages: Set[String]): Boolean = {
    acceptedLanguages(new ULocale(lang).getLanguage)
  }

  def bySite(items: Iterable[WikiSiteLink],
             acceptedSite: Set[String]): Iterable[WikiSiteLink] = {
    items.filter(i => acceptedSite(i.site))
  }

  def byLanguages(items: Iterable[WikiSiteLink],
                  acceptedLanguages: Set[String]): Iterable[WikiSiteLink] = {
    bySite(items, acceptedLanguages.flatMap(langToSites(_, "wikipedia")))
  }

  def literalByLanguages(
      items: Iterable[ItemLangLiteral],
      acceptedLanguages: Set[String]): Iterable[ItemLangLiteral] = {
    items.filter(i => filterLocale(i.lang, acceptedLanguages))
  }
}
