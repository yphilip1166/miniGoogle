import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;

/* Followed design pattern on
 * https://docs.oracle.com/cd/E17277_02/html/GettingStartedGuide/BerkeleyDB-JE-GSG.pdf
 * page 39
 */
public class UserDA {
    private PrimaryIndex<String,User> pIdx;

    public UserDA(EntityStore store){
        pIdx = store.getPrimaryIndex(String.class, User.class);
    }

    public void store(User user){
        pIdx.put(user);
    }

    public User get(String username){
        return pIdx.get(username);
    }

    public boolean contains(String username){
        return pIdx.contains(username);
    }
}
