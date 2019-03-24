package me.erickguan.kgdoc.tasks

import java.nio.file.Paths

import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, SideInput, SideSet}
import me.erickguan.kgdoc.extractors.ItemLangLiteral
import me.erickguan.kgdoc.filters.WikidataFilter
import me.erickguan.kgdoc.json.WikidataItem
import me.erickguan.kgdoc.processors.DatasetLineProcessor

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
    * Extracts all classes from wikidata items.
    * @param items all items from Wikidata
    * @param checkpointPath a path to checkpoint
    * @return all classes from wikidata items
    */
  def extractClasses(items: SCollection[WikidataItem],
                     checkpointPath: String): SCollection[WikidataItem] = {
    import com.spotify.scio.extra.checkpoint._

    sc.checkpoint(checkpointPath) {
      items.filter(
        WikidataFilter.bySubclass(WikidataFilter.SubclassPropertyId, _))
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
      .filter(WikidataFilter.byBibliographicClass)
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
        !WikidataFilter.byInstanceOfEntities(excludeClasses, item)
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
      items: SCollection[ItemLangLiteral],
      entitySide: SideInput[Set[String]],
      relationSide: SideInput[Set[String]]
  ): SCollection[ItemLangLiteral] = {
    items
      .withSideInputs(entitySide, relationSide)
      .filter { (l, ctx) =>
        val entities = ctx(entitySide)
        val relations = ctx(relationSide)
        entities(l.item) || relations(l.item)
      }
      .toSCollection
  }

  val DatasetFiles = List("train.txt", "valid.txt", "test.txt")

  /**
    * Extracts triples from a dataset.
    * @param classes all classes from Wikidata
    * @param checkpointPath a path to checkpoint
    * @return a (s, p, o) tuple of data
    */
  def triplesFromDataset(
      datasetPath: String,
      checkpointPath: String): SCollection[(String, String, String)] = {
    import com.spotify.scio.extra.checkpoint._

    val tripleSources = DatasetFiles.map { n =>
      sc.textFile(Paths.get(datasetPath, n).toString)
        .map(DatasetLineProcessor.decodeLine(_))
    }
    sc.checkpoint(checkpointPath + "-dataset") {
      sc.unionAll(tripleSources)
    }
  }

  /**
    * Extracts entities and relations from triples collection
    * @param classes all classes from Wikidata
    * @return two side sets for entities and relations
    */
  def entityAndRelationSideSet(triples: SCollection[(String, String, String)])
    : (SideSet[String], SideSet[String]) = {
    val entities = triples.flatMap(t => Seq(t._1, t._3)).toSideSet
    val relations = triples.map(_._2).toSideSet

    (entities, relations)
  }

}
