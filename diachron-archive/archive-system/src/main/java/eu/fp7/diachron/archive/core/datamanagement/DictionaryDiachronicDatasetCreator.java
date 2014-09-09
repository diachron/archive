package eu.fp7.diachron.archive.core.datamanagement;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import eu.fp7.diachron.archive.models.DiachronicDataset;
import eu.fp7.diachron.archive.models.RDFDiachronicDataset;

/**
 * 
 * Implements the DataStatement interface for Virtuoso specific data statements.
 *
 */
public class DictionaryDiachronicDatasetCreator implements DiachronicDatasetCreator {
  private final DictionaryService dictionary;

  public DictionaryDiachronicDatasetCreator(DictionaryService dictionary) {
    this.dictionary = dictionary;
  }

  /**
   * Creates a new diachronic dataset and associates it with metadata defined in the input
   * parameter.
   * 
   * @param metadata A set of metadata to be associated with the new diachronic dataset.
   * @throws IOException
   */
  public String createDiachronicDataset(ArchiveEntityMetadata metadata) throws IOException {

    // TODO create with factory
    DiachronicDataset diachronicDataset = new RDFDiachronicDataset(dictionary.createDiachronicDatasetId());
    HashMap<String, String> metadataMap = metadata.getMetadataMap();
    Set<String> keySet = metadataMap.keySet();

    for (String predicate : keySet) {
      diachronicDataset.setMetaProperty(predicate, metadataMap.get(predicate));
    }
    dictionary.storeDiachronicDataset(diachronicDataset);

    return diachronicDataset.getId();
  }

}
