package org.athena.imis.diachron.archive.core.dataloader;

import virtuoso.jdbc4.VirtuosoDataSource;

public final class StoreFactory {
	public static DictionaryService createCachedDictionaryService(DictionaryService dictionaryService) {
		DictionaryCache cache =  new DictionaryCache(dictionaryService);
		cache.init();
		return cache;
	}
	
	public static DictionaryService createPersDictionaryService(VirtuosoDataSource dataSource) {
		return new RDFDictionary(dataSource);
	}

}
