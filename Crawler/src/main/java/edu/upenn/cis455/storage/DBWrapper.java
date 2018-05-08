package edu.upenn.cis455.storage;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

import java.io.File;

public class DBWrapper {
	
	private static DBWrapper db_instance= null; 
	private static String envDirectory = null;
	private static Environment myEnv;
	private static EntityStore store;
	private static PageDA pda = null;
	private static DiskQueueDA dda = null;
    private static ContentFileDA fda = null;
	
	/* TODO: write object store wrapper for BerkeleyDB */
	private DBWrapper(String path){
		System.out.println("Setting: " + path);
		this.envDirectory = path;
	}
	
	public static void initDBStore(String path){
		if (db_instance == null){
			db_instance = new DBWrapper(path);
			System.out.println("Env dir is now: " + path);
			db_instance.init();
		}
	}
	
	public void init(){
		setup();
		setupDA();
	}
	
	private void setup(){
		File envHome = new File(this.envDirectory);
		envHome.mkdir();
		
		EnvironmentConfig envConfig = new EnvironmentConfig();
		StoreConfig storeConfig = new StoreConfig();
		
		envConfig.setAllowCreate(true);
		storeConfig.setAllowCreate(true);
		
		envConfig.setTransactional(true);
		storeConfig.setTransactional(true);
		
		myEnv = new Environment(envHome, envConfig);
		store = new EntityStore(myEnv, "EntitiStore", storeConfig);
	}
	
	private void setupDA(){
		pda = new PageDA(this.store);
		dda = new DiskQueueDA(this.store);
        fda = new ContentFileDA(this.store);
	}
	
	public static void putPage(Page pg){
		pda.put(pg);
	}
	
	public static Page getPage(String addr){
		return pda.get(addr);
	}
	
	public static boolean containsPage(String addr){
		return pda.containsFile(addr);
	}

	public static void putQueue(DiskQueue dq){
		dda.put(dq);
	}

	public static DiskQueue getQueue(String name){
		return dda.get(name);
	}

	public static boolean containsQueue(String name){
		return dda.containsQueue(name);
	}

	public static void putFile(ContentFile cf) {
        fda.put(cf);
    }

    public static ContentFile getFile(String contentId){
        return fda.get(contentId);
    }

    public static boolean containsFile(String contentId){
        return fda.containsFile(contentId);
    }

	public static void shutdown(){
		store.close();
		myEnv.close();
	}
}
