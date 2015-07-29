package org.athena.imis.diachron.archive.core.dataloader;

public class StoreFactory {
	public static DictionaryService createDictionaryService() {
		return new DictionaryCache(new RDFDictionary());
	}
	
	public static Loader createDataLoader() {
		return new VirtLoader();
	}
	
	public static Remover createDataRemover() {
		return new VirtRemover();
	}
	
	static DictionaryService createPersDictionaryService() {
		return new RDFDictionary();
	}

}
