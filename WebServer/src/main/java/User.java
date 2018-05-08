import java.util.HashSet;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class User {
    @PrimaryKey
    private String userName;
    private byte[] password;
    private HashSet<String> subscriptions;

    public User(){

    }

    public User(String userName, byte[] password){
        this.userName = userName;
        this.password = password;
        subscriptions = new HashSet<>();
    }

    public void setPassword(byte[] password){
        this.password = password;
    }

    public String getUsername(){
        return userName;
    }

    public byte[] getPassword(){
        return password;
    }

    public void subscribe(String channel){
        subscriptions.add(channel);
    }

    public void unsubscribe(String channel){
        subscriptions.remove(channel);
    }

    public boolean isSubscribed(String channel){
        return subscriptions.contains(channel);
    }
}
