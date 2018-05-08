package edu.upenn.cis455.database;

import java.io.File;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

public class DBWrapper {
	private static String envDirectory = null;
	
	private static Environment myEnv;
	private static EntityStore store;
	
	private Object lock = new Object();
	
	public DBWrapper() {}
	
	public void setup(String edir, String stName, boolean readonly) {
		
		if(!stName.equals("indexerDB")) {
			System.out.println("no such EntityStore!");
			return;
		}
		
		envDirectory = edir;
		File envHome = new File(envDirectory);
		if(!envHome.exists()) envHome.mkdirs();
		
		EnvironmentConfig myEnvConfig = new EnvironmentConfig();
		StoreConfig storeConfig = new StoreConfig();
		
		myEnvConfig.setAllowCreate(!readonly);
		storeConfig.setAllowCreate(!readonly);

		myEnv = new Environment(new File(envDirectory), myEnvConfig);
		store = new EntityStore(myEnv, stName, storeConfig);
	}

	//Close both Environment and EntityStore
	public void close() {
		
		if(store != null) {
			try	{
				store.close();
			} catch (DatabaseException e) {
				System.err.println("Error closing store: " + e.toString());
				System.exit(-1);
			}
		}
		
		if(myEnv != null) {
			try	{
				myEnv.close();
			} catch (DatabaseException e) {
				System.err.println("Error closing myEnv: " + e.toString());
				System.exit(-1);
			}
		}
	}
	
	// Place objects in EntityStore
	public void put(WordEntity obj) {
		synchronized(lock) {
			PrimaryIndex<String, WordEntity> pIdx = store.getPrimaryIndex(String.class, WordEntity.class);
			pIdx.put(obj);
		}
	}
	
	// Retrieve objects from EntityStore
	public WordEntity get(String key) {
		PrimaryIndex<String, WordEntity> pIdx = store.getPrimaryIndex(String.class, WordEntity.class);
		return pIdx.get(key);
	}
	
	// Delete Entity Objects
	public void remove(String key) {
		PrimaryIndex<String, WordEntity> pIdx = store.getPrimaryIndex(String.class, WordEntity.class);
		pIdx.delete(key);
	}
	
	public boolean contains(String key) {
		PrimaryIndex<String, WordEntity> pIdx = store.getPrimaryIndex(String.class, WordEntity.class);
		return pIdx.contains(key);
	}
}
