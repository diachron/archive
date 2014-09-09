package org.athena.imis.diachron.archive.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.athena.imis.diachron.archive.core.dataloader.ArchiveEntityMetadata;
import org.athena.imis.diachron.archive.core.dataloader.DictionaryService;
import org.athena.imis.diachron.archive.core.dataloader.VirtLoader;
import org.athena.imis.diachron.archive.models.DiachronicDataset;
import org.athena.imis.diachron.archive.models.RDFDiachronicDataset;

/**
 * 
 * Implements the DataStatement interface for Virtuoso specific data statements.
 *
 */
public class VirtDataStatement implements DataStatement {
  private final VirtLoader virtLoader;
  private final DictionaryService dictionary;

  public VirtDataStatement(VirtLoader virtLoader, DictionaryService dictionary) {
    this.virtLoader = virtLoader;
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
    DiachronicDataset diachronicDataset = new RDFDiachronicDataset();
    HashMap<String, String> metadataMap = metadata.getMetadataMap();
    Set<String> keySet = metadataMap.keySet();

    for (String predicate : keySet) {
      diachronicDataset.setMetaProperty(predicate, metadataMap.get(predicate));
    }
    return dictionary.createDiachronicDataset(diachronicDataset);
  }

}
