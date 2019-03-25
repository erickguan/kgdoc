package me.erickguan.kgdoc.filters

import me.erickguan.kgdoc.json.SiteLink

object WikidataSiteLinkFilter {
  // language in Wikidata is different than site identifier
  def langToSite(lang: String): String = {
    lang
  }
}

class WikidataSiteLinkFilter(link: SiteLink) {
  def byLanguage(lang: String): Boolean =
    WikidataSiteLinkFilter.langToSite(lang) == link.site
}
