package me.erickguan.kgdoc.filters

import com.ibm.icu.util.ULocale
import me.erickguan.kgdoc.extractors.{ItemLangLiteral, WikiSiteLink}
import me.erickguan.kgdoc.processors.WikiSite

object WikidataSiteFilter {
  def langToSites(lang: String, code: String): Iterable[String] = {
    try {
      WikiSite.byLanguage(lang).filter(_("code") == code).map(_("dbname"))
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
