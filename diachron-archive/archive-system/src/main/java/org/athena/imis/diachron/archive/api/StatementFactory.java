package org.athena.imis.diachron.archive.api;

import org.athena.imis.diachron.archive.core.dataloader.StoreFactory;
import org.athena.imis.diachron.archive.core.dataloader.VirtLoader;

import virtuoso.jdbc4.VirtuosoDataSource;

/**
 * A factory class for creating query statements, i.e. objects of classes that implement the
 * QueryStatement interface.
 *
 */
public class StatementFactory {

  /**
   * Creates a Virtuoso query statement.
   * 
   * @return A new VirtQueryStatement object.
   */
  public static QueryStatement createQueryStatement(VirtuosoDataSource dataSource) {
    return new VirtQueryStatement(dataSource);
  }

  /**
   * Creates a Virtuoso data statement.
   * 
   * @param dataSource
   * @return A new VirtDataStatement object.
   */
  public static DataStatement createVirtuosoDataStatement(VirtuosoDataSource dataSource) {
    return new VirtDataStatement(new VirtLoader(dataSource),
        StoreFactory.createCachedDictionaryService(StoreFactory
            .createPersDictionaryService(dataSource)));
  }

}
