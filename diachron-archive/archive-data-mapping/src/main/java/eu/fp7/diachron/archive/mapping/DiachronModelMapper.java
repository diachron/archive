package eu.fp7.diachron.archive.mapping;

import java.io.IOException;

/**
 * Defines the necessary functionalities to map a dataset to the Diachron model.
 * 
 * @author Ruben Navarro Piris
 *
 */
public interface DiachronModelMapper {

  void executeTboxMapping(String graphUri) throws IOException;

  void executeAboxMapping(String graphUri, boolean mapDataset) throws IOException;
}
