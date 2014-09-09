package eu.fp7.diachron.archive;

public class ArchiveApp {
	static ArchiveApp app = null;
	private ArchiveApp() {
		
	}
	
	public static ArchiveApp getInstance() {
		if (app == null)
			app = new ArchiveApp();
		return app;
	}
	
	public void init() {
		//TODO initialize the archive
		
	}
	
	public boolean isInitialized() {
		//TODO impl
		return false;
	}
}
