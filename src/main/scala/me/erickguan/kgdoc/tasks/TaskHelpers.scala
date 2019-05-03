package me.erickguan.kgdoc.tasks

import java.nio.file.Paths

import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, SideInput, SideSet}
import me.erickguan.kgdoc.extractors.ItemLangLiteral
import me.erickguan.kgdoc.filters.{
  LatentFeatureModelDatasetFilter,
  WikidataItemFilter
}
import me.erickguan.kgdoc.json.WikidataItem
import me.erickguan.kgdoc.processors.DatasetLineProcessor
import org.apache.beam.sdk.metrics.Counter

class TaskHelpers(sc: ScioContext) {

  /**
    * Extracts all items from wikidata JSON dump.
    * @param path path to a JSON dump
    * @return a collection of wikidata items
    */
  def extractItems(path: String): SCollection[WikidataItem] = {
    import me.erickguan.kgdoc.processors.WikidataJsonDumpLineProcessor

    sc.textFile(path)
      .filter(WikidataJsonDumpLineProcessor.filterNonItem)
      .map(WikidataJsonDumpLineProcessor.decodeJsonLine)
  }

  /**
    * Extracts all items from wikidata JSON dump with counting metrics.
    * @param path path to a JSON dump
    * @param counter a counter for counting the number of items
    * @return a collection of wikidata items
    */
  def extractItems(path: String,
                   counter: Counter): SCollection[WikidataItem] = {
    import me.erickguan.kgdoc.processors.WikidataJsonDumpLineProcessor

    sc.textFile(path)
      .filter { i =>
        counter.inc()
        WikidataJsonDumpLineProcessor.filterNonItem(i)
      }
      .map(WikidataJsonDumpLineProcessor.decodeJsonLine)
  }

  /**
    * Extracts all classes from wikidata items.
    * @param items all items from Wikidata
    * @param checkpointPath a path to checkpoint
    * @return all classes from wikidata items
    */
  def extractClasses(items: SCollection[WikidataItem],
                     checkpointPath: String): SCollection[WikidataItem] = {
    import com.spotify.scio.extra.checkpoint._

    sc.checkpoint(checkpointPath) {
      items.filter { i =>
        val h = new WikidataItemFilter(i)
        h.bySubclass(WikidataItemFilter.SubclassPropertyId)
      }
    }
  }

  /**
    * Extracts a collection of bibliographic class from wikidata items.
    * wikicite data are too many now. we don't need it now
    * @param classes all classes from Wikidata
    * @return a sideinput of classes from wikidata items
    */
  def bibliographicClassesSideInput(
      classes: SCollection[WikidataItem]): SideInput[Iterable[String]] = {
    classes
      .filter(new WikidataItemFilter(_).byBibliographicClass)
      .map(_.id)
      .asIterableSideInput
  }

  /**
    * Filter a collection without bibliographic items
    * wikicite data are too many now. we don't need it now
    * @param items all items collection
    * @param bcSide a side input including bibliographic items
    * @return a collection filtered
    */
  def filteredBibliographicClasses(
      items: SCollection[WikidataItem],
      bcSide: SideInput[Iterable[String]]): SCollection[WikidataItem] = {
    items
      .withSideInputs(bcSide)
      .filter { (item, ctx) =>
        val excludeClasses = ctx(bcSide)
        !new WikidataItemFilter(item).byInstanceOfEntities(excludeClasses)
      }
      .toSCollection
  }

  /**
    * Filter a collection without bibliographic items with counter
    * wikicite data are too many now. we don't need it now
    * @param items all items collection
    * @param bcSide a side input including bibliographic items
    * @param counter a counter for number of dropped items
    * @return a collection filtered
    */
  def filteredBibliographicClasses(
      items: SCollection[WikidataItem],
      bcSide: SideInput[Iterable[String]],
      counter: Counter): SCollection[WikidataItem] = {
    items
      .withSideInputs(bcSide)
      .filter { (item, ctx) =>
        val excludeClasses = ctx(bcSide)
        val result =
          !new WikidataItemFilter(item).byInstanceOfEntities(excludeClasses)
        if (!result) {
          counter.inc()
        }
        result
      }
      .toSCollection
  }

  /**
    * Filter a collection without items in the given dataset
    * @param items all items collection
    * @param entitySide a side input including all entity id
    * @param relationSide a side input including all relation id
    * @return a collection filtered
    */
  def filteredDataset(
      items: SCollection[WikidataItem],
      entitySide: SideInput[Set[String]],
      relationSide: SideInput[Set[String]]
  ): SCollection[WikidataItem] = {
    items
      .withSideInputs(entitySide, relationSide)
      .filter { (l, ctx) =>
        val entities = ctx(entitySide)
        val relations = ctx(relationSide)
        entities(l.id) || relations(l.id)
      }
      .toSCollection
  }

  val DatasetFiles = List("train.txt", "valid.txt", "test.txt")

  /**
    * Extracts triples from a dataset.
    * @param datasetPath a path to dataset
    * @param checkpointPath a path to checkpoint
    * @return a (s, p, o) tuple of data
    */
  def triplesFromDataset(
      datasetPath: String,
      checkpointPath: String): SCollection[(String, String, String)] = {
    import com.spotify.scio.extra.checkpoint._

    val tripleSources = DatasetFiles.map { n =>
      sc.textFile(datasetPath + n)
        .map(DatasetLineProcessor.decodeLine(_))
    }
    sc.checkpoint(checkpointPath + "-dataset") {
      sc.unionAll(tripleSources)
    }
  }

  /**
    * Extracts entities from a plain text translation file.
    * @param datasetPath a path to dataset
    * @return a iterable side input of entities map
    */
  def entitiesFromPlaintextTranslation(datasetPath: String): SideInput[Map[String, Long]] = {
    val EntityTranslationFile = "entities.txt"

    sc.textFile(datasetPath + EntityTranslationFile)
      .map { l =>
        val spans = l.split('\t')
        (spans(0), spans(1).toLong) // we only have two elements there
      }
      .asMapSideInput
  }

  /**
    * Extracts entities and relations from triples collection
    * @param triples all classes from Wikidata
    * @return two side sets for entities and relations
    */
  def entityAndRelationSideSet(triples: SCollection[(String, String, String)])
    : (SideSet[String], SideSet[String]) = {
    val entities = triples.flatMap(t => Seq(t._1, t._3)).toSideSet
    val relations = triples.map(_._2).toSideSet

    (entities, relations)
  }

  /**
    * Remove deficit relations based on the threshold that considered to be enough
    * @param triples all triples
    * @param relationThreshold the number of items
    * @return filtered triples
    */
  def removeDeficitRelation(
      triples: SCollection[(String, String, String)],
      relationThreshold: Long): SCollection[(String, String, String)] = {
    val validRelationSide =
      triples
        .map(LatentFeatureModelDatasetFilter.relation)
        .countByValue
        .filter(LatentFeatureModelDatasetFilter
          .applyThresholdOnRelation(_, relationThreshold))
        .map(_._1)
        .toSideSet

    triples
      .withSideInputs(validRelationSide.side)
      .filter { (t, ctx) =>
        val validRelation = ctx(validRelationSide.side)
        validRelation(t._2)
      }
      .toSCollection
  }

}
