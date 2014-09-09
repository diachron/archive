package eu.fp7.diachron.archive.core.datamanagement;

import java.io.IOException;

import eu.fp7.diachron.archive.core.store.Loader;
import eu.fp7.diachron.archive.core.store.SparqlStore;
import eu.fp7.diachron.archive.core.store.VirtuosoLoader;
import eu.fp7.diachron.archive.core.store.VirtuosoSparqlStore;
import virtuoso.jdbc4.VirtuosoDataSource;

public final class StoreFactory {
	public static DictionaryService createCachedDictionaryService(DictionaryService dictionaryService) throws IOException {
		DictionaryCache cache =  new DictionaryCache(dictionaryService);
		cache.init();
		return cache;
	}
    
    public static DictionaryService createVirtuosoDictionaryService(VirtuosoDataSource dataSource) {
        return new RDFDictionary(new VirtuosoSparqlStore(dataSource), new VirtuosoLoader(dataSource));
    }
    
    public static DictionaryService createPersDictionaryService(SparqlStore sparqlStore, Loader loader) {
        return new RDFDictionary(sparqlStore, loader);
    }

}
