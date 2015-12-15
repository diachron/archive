package org.athena.imis.diachron.archive.core.dataloader;

/**
 * Factory class for the DictionaryService implementations.
 * @author Marios Meimaris
 *
 */
public class StoreFactory {
	
	//private static DictionaryService dict = null;
	/**
	 * Creates a new Dictionary Cache and RDF dictionary (persistent storage) object. 
	 * @return
	 */
	public static DictionaryService createDictionaryService() {
		/*if(dict == null)
			dict = new DictionaryCache(new RDFDictionary());					
		return dict;*/
		return new DictionaryCache(new RDFDictionary());
	}
	
	/**
	 * Creates a new data loader.
	 * @return A Loader implementation (this version: Virtuoso Loader)
	 */
	public static Loader createDataLoader() {
		return new VirtLoader();
	}
	
	/**
	 * Creates a new data remover.
	 * @return A Remover implementation (this version: Virtuoso Remover)
	 */
	public static Remover createDataRemover() {
		return new VirtRemover();
	}
	
	/**
	 * Creates a new persistent storage service
	 * @return A new RDFDictionary object linked to the archive.
	 */
	static DictionaryService createPersDictionaryService() {
		return new RDFDictionary();
	}

}
