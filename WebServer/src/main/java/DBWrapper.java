import java.io.File;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.List;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

public class DBWrapper {
    static DBWrapper instance = null;

    private static String envDirectory = null;

    private static Environment myEnv;
    private static EntityStore store;

    private UserDA userDA;

    public DBWrapper(String dir){
        envDirectory = dir;
        setup();
    }

    private void setup(){
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);

        File directory = new File(envDirectory);
        //automatically check if dir already exists
        directory.mkdir();

        myEnv = new Environment(directory, envConfig);

        StoreConfig storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setTransactional(true);
        store = new EntityStore(myEnv, "EntityStore", storeConfig);

        userDA = new UserDA(store);
    }

    //singleton pattern to ensure there is only one instance of db
    public static synchronized DBWrapper getInstance(String dir){
        if(instance == null){
            instance = new DBWrapper(dir);
        }
        return instance;
    }



    public User getUser(String username){
        return userDA.get(username);
    }


    public void storeUser(String username, String password)
            throws NoSuchAlgorithmException, UnsupportedEncodingException{
        byte[] encrypted = encrypt(password);
        User user = new User(username, encrypted);
        userDA.store(user);
    }

    public void updateUser(User user){
        userDA.store(user);
    }

    public boolean containsUser(String username){
        return userDA.contains(username);
    }

    //encrypt passwords
    public byte[] encrypt(String msg) throws NoSuchAlgorithmException, UnsupportedEncodingException{
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        return messageDigest.digest(msg.getBytes("UTF-8"));
    }

    //check if username/password combination is correct
    public boolean checkCombination(String username, String password)
            throws NoSuchAlgorithmException, UnsupportedEncodingException{
        byte[] correctPas = userDA.get(username).getPassword();
        byte[] inputPas = encrypt(password);

        if(correctPas.length != inputPas.length){
            return false;
        }
        for(int i=0; i<correctPas.length; i++){
            if(correctPas[i] != inputPas[i]){
                return false;
            }
        }
        return true;
    }

    public void sync(){
        if(store != null){
            store.sync();
        }

        if(myEnv != null){
            myEnv.sync();
        }
    }

    public void shutdown(){
        if(instance == null){
            return;
        }

        // It is recommended that you close your store before you close your environment
        try{
            if(store != null){
                store.close();
            }
        }catch (DatabaseException dbe){
            System.err.println("Error closing store: " + dbe.toString());
        }

        try{
            if(myEnv != null){
                myEnv.cleanLog();
                myEnv.close();
            }
        } catch (DatabaseException dbe){
            System.err.println("Error closing store: " + dbe.toString());
        }
        instance = null;
    }
}
