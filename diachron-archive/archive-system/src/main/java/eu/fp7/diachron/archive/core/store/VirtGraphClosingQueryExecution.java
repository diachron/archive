package eu.fp7.diachron.archive.core.store;

import java.io.Closeable;

import virtuoso.jena.driver.VirtGraph;

import com.hp.hpl.jena.query.QueryExecution;

/**
 * Wrapper class for {@link QueryExecution}, overrides the {@link QueryExecution#close()} method to,
 * additionally, close the configured {@link VirtGraph} class, forwards all other methods.
 * 
 * On creation, a VirtGraph gets a connection from the dataSource and uses it for its lifetime
 * (until closed). Therefore, to avoid having "dead connections", the virtGraph object's lifetime is
 * limited to the query lifetime
 * 
 * @author Ruben Navarro Piris
 *
 */
public final class VirtGraphClosingQueryExecution extends ForwardingQueryExecution implements
    QueryExecution, Closeable {
  private final VirtGraph virtGraph;

  public VirtGraphClosingQueryExecution(QueryExecution queryExecution, VirtGraph virtGraph) {
    super(queryExecution);
    this.virtGraph = virtGraph;
  }

  /** Extended functionality **/

  @Override
  public void close() {
    try {
      this.virtGraph.close();
    } finally {
      super.close();
    }
  }

}
