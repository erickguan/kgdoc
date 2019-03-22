package me.erickguan.kgdoc.extractors

import me.erickguan.kgdoc.KgdocFixtureSuite
import me.erickguan.kgdoc.json.WikidataItem
import me.erickguan.kgdoc.processors.WikidataJsonDumpLineProcessor
import org.scalatest.{Outcome, Tag}

import scala.io.Source

class WikidataExtractorRegressionSuite extends KgdocFixtureSuite {
  import WikidataExtractor._

  case class FixtureParam(items: Seq[WikidataItem])

  def withFixture(test: OneArgTest): Outcome = {
    val source = Source.fromURL(
      getClass.getResource("/fixtures/wikidata-regression.json"))
    val items = source.getLines
      .filter(WikidataJsonDumpLineProcessor.filterNonItem)
      .map(WikidataJsonDumpLineProcessor.decodeJsonLine)
      .toSeq
    val fixtureParam = FixtureParam(items)
    try withFixture(test.toNoArgTest(fixtureParam)) // Invoke the test function
    finally {
      // Perform cleanup
    }
  }

  test("can extract things", Tag("extraction")) { f =>
    assert(
      f.items.flatMap(triples).nonEmpty
    )
    assert(f.items.flatMap(labelLiterals).nonEmpty)
    assert(f.items.flatMap(descriptions).nonEmpty)
    assert(f.items.flatMap(sitelinks).nonEmpty)
  }
}
