import javax.xml.stream.FactoryConfigurationError;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.Date;

import static java.util.Collections.sort;

public class MySQLWrapper implements IndexingSQL{
    private Connection connect = null;
    private Statement statement = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;

    public static int TOPK_PERWORD = 2500;
    public static float TF_WEIGHT = 0.14f;
    public static int RESULT_NUMBER = 90;
    public static float WORD_NEIGHBOR_WEIGHT = 1200.0f;
    public static float USER_PROFILE_WEIGHT = 1.5f;
    public static int TITLE_IMPORTANCE = 5;
    public static float PAGE_RANK_WEIGHT = 0.1f;

    public static void tuneParameter(int topk, float tfWeight, int resultNumber, float neighborWeight, float profileWeight, int titleImportance, float pageRankScore){
        TOPK_PERWORD = topk;
        TF_WEIGHT = tfWeight;
        RESULT_NUMBER = resultNumber;
        WORD_NEIGHBOR_WEIGHT = neighborWeight;
        USER_PROFILE_WEIGHT = profileWeight;
        TITLE_IMPORTANCE = titleImportance;
        PAGE_RANK_WEIGHT = pageRankScore;
    }

    private void checkInput(String word, String url) throws IllegalArgumentException{
        if(word == null || url == null || word.length() == 0 || url.length() == 0){
            throw new IllegalArgumentException("word or url is null or empty: word: "+word + " url: " + url);
        } else if (word.length() > 32 || url.length() > 256){
            throw new IllegalArgumentException("word or url is too long: "+word.length() + " " + url.length());
        }
    }
    public static String getHost(String urlString){
        try {
            URL url = new URL(urlString);
            return url.getHost();
        } catch (MalformedURLException e){
            System.err.println("Malformed url:" + e.toString());
            return null;
        }
    }

    public void cleanResult(){
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null){
                preparedStatement.close();
            }
            if(statement!=null){
                statement.close();
            }
        } catch (SQLException e){}
    }

    public HashMap<String, Float> pageRankMap;

    public void loadPageRank() {
        try{
            pageRankMap = new HashMap<String, Float>();
            BufferedReader br = new BufferedReader(new FileReader("data/PageRank.csv"));
            while(true){
                String line = br.readLine();
                if(line==null)break;
                String[] parts = line.split(", ");
                if(parts==null || parts.length!=2)
                    break;
                pageRankMap.put(parts[0], Float.valueOf(parts[1]));
            }
        } catch (FileNotFoundException e){
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
        System.out.println("PageRank Number: "+pageRankMap.size());
    }



    public boolean Initialize(){
        loadPageRank();
            try{
                if(connect == null){
                    Class.forName("com.mysql.cj.jdbc.Driver");
                    System.out.println("Connecting database server");
                    connect = DriverManager.getConnection(
                            "jdbc:mysql://db555mysql.c7zwwxbxqnoc.us-east-1.rds.amazonaws.com/db555",
                            "db555zhenlinghan",
                            "db555password" );
                }
                return true;
            } catch (Exception e){
                System.out.println("Can't connect to database: " + e.toString());
                return false;
            }
    }
    public boolean putRecord(String word, String url,
                             int tf,
                             float capitalPercent,
                             float titlePercent,
                             float linkPercent,
                             float emphasisPercent,
                             float metaPercent,
                             float headingScore,
                             float positionScore,
                             int p1, int p2, int p3, int p4, int p5) throws IllegalArgumentException {
        boolean result = true;
        checkInput(word, url);
        try {
            // --------Write---------
            // PreparedStatements can use variables and are more efficient
            preparedStatement = connect
                    .prepareStatement("insert into  Indexing values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            // "myuser, webpage, datum, summary, COMMENTS from feedback.comments");
            // Parameters start with 1
            preparedStatement.setString(1, word);
            preparedStatement.setString(2, url);
            preparedStatement.setInt(3, tf);
            preparedStatement.setFloat(4, capitalPercent);
            preparedStatement.setFloat(5, titlePercent);
            preparedStatement.setFloat(6, linkPercent);
            preparedStatement.setFloat(7, emphasisPercent);
            preparedStatement.setFloat(8, metaPercent);
            preparedStatement.setFloat(9, headingScore);
            preparedStatement.setFloat(10, positionScore);
            preparedStatement.setInt(11,p1);
            preparedStatement.setInt(12,p2);
            preparedStatement.setInt(13,p3);
            preparedStatement.setInt(14,p4);
            preparedStatement.setInt(15,p5);
            preparedStatement.executeUpdate();
            result = true;
        } catch (SQLException e){
            System.out.println("Error inserting record: "+e.toString());
            result = false;
        } finally{
            cleanResult();
        }
        return result;
    }
    public IndexingItem getRecord(String word, String url)throws IllegalArgumentException{
        checkInput(word, url);

        return null;
    }
    public List<IndexingItem> getAllRecords(){
        try {
            statement = connect.createStatement();
            // Result set get the result of the SQL query
            resultSet = statement.executeQuery("select * from Indexing");
            writeResultSet(resultSet);
        } catch (SQLException e){
            System.out.println(e.toString());
        }
        return null;
    }
    public MySQLWrapper(){
        Initialize();
    }

    public boolean deleteRecord(String word, String url){
        checkInput(word, url);
        try{
            // --------Delete---------
            // Remove again the insert comment
            preparedStatement = connect
                    .prepareStatement("delete from Indexing where word= ? and url = ?;");
            preparedStatement.setString(1, "Peking");
            preparedStatement.setString(2, "www.baidu.com");
            preparedStatement.executeUpdate();
            return true;
        } catch (SQLException e){
            System.out.println("error deleting record, word: " + word + " url: " + url);
            return false;
        } finally{
            cleanResult();
        }
    }




    private void writeMetaData(ResultSet resultSet) throws SQLException {
        //  Now get some metadata from the database
        // Result set get the result of the SQL query

        System.out.println("The columns in the table are: ");

        System.out.println("Table: " + resultSet.getMetaData().getTableName(1));
        for  (int i = 1; i<= resultSet.getMetaData().getColumnCount(); i++){
            System.out.println("Column " +i  + " "+ resultSet.getMetaData().getColumnName(i));
        }
    }

    private void writeResultSet(ResultSet resultSet) throws SQLException {
        // ResultSet is initially before the first data set
        while (resultSet.next()) {
            // It is possible to get the columns via name
            // also possible to get the columns via the column number
            // which starts at 1
            // e.g. resultSet.getString(2);
            String word = resultSet.getString("word");
            String url = resultSet.getString("url");
            int tf = resultSet.getInt("tf");
            Float cp = resultSet.getFloat("capitalPercent");
            System.out.println("word: " + word + " url: " + url + " tf: " + tf + " cp: " + cp);
        }
    }

    private List<IndexingItem> retreiveIndexingItem(ResultSet resultSet) throws SQLException {
        // ResultSet is initially before the first data set
        ArrayList<IndexingItem> list = new ArrayList<>();
        while (resultSet.next()) {
            // It is possible to get the columns via name
            // also possible to get the columns via the column number
            // which starts at 1
            // e.g. resultSet.getString(2);
            String word = resultSet.getString(1);
            String url = resultSet.getString(2);
            int tf = resultSet.getInt(3);
            float capitalPercent = resultSet.getFloat(4);
            float titlePercent = resultSet.getFloat(5);
            float linkPercent = resultSet.getFloat(6);
            float emphasisPercent = resultSet.getFloat(7);
            float metaPercent = resultSet.getFloat(8);
            float headingScore = resultSet.getFloat(9);
            float positionScore = resultSet.getFloat(10);
            int p1 = resultSet.getInt(11);
            int p2 = resultSet.getInt(12);
            int p3 = resultSet.getInt(13);
            int p4 = resultSet.getInt(14);
            int p5 = resultSet.getInt(15);
            float scoreSum = resultSet.getFloat(16);
            IndexingItem item = new IndexingItem(word, url, tf,
                    capitalPercent, titlePercent, linkPercent, emphasisPercent,
                    metaPercent, headingScore, positionScore, p1, p2, p3, p4, p5, scoreSum);
            list.add(item);
        }
        return list;
    }

    private List<IndexingItem> retrieveIndexingItemCache(ResultSet resultSet) throws SQLException {
        ArrayList<IndexingItem> list = new ArrayList<>();
        while (resultSet.next()) {
            // It is possible to get the columns via name
            // also possible to get the columns via the column number
            // which starts at 1
            // e.g. resultSet.getString(2);
            String word = resultSet.getString(1);
            String url = resultSet.getString(2);
            float scoreSum = resultSet.getFloat(3);
            IndexingItem item = new IndexingItem(word, url, 1,
                    0, 0, 0, 0,
                    0, 0, 0, -1, -1, -1, -1, -1, scoreSum);
            list.add(item);
        }
        return list;
    }

    public void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }

            if (statement != null) {
                statement.close();
            }

            if (connect != null) {
                connect.close();
            }
        } catch (Exception e) {

        }
    }

    // Login Database Management
    private String sha256Hash(String plain){
        String strResult = null;
        if (plain != null && plain.length() > 0)
        {
            try
            {
                MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
                messageDigest.update(plain.getBytes());
                byte byteBuffer[] = messageDigest.digest();
                StringBuffer strHexString = new StringBuffer();
                for (int i = 0; i < byteBuffer.length; i++)
                {
                    String hex = Integer.toHexString(0xff & byteBuffer[i]);
                    if (hex.length() == 1)
                    {
                        strHexString.append('0');
                    }
                    strHexString.append(hex);
                }
                strResult = strHexString.toString();
            }
            catch (NoSuchAlgorithmException e)
            {
                e.printStackTrace();
            }
        }
        return strResult;
    }

    public boolean register(String username, String password){
        if(username == null || password == null || username.length()==0 || password.length()==0){
            System.err.println("Register Failed, username or password Illegal");
            throw new IllegalArgumentException("Register Failed, username or password Illegal");
        }
        try {
            preparedStatement = connect.prepareStatement("INSERT INTO Users VALUES(?,?,\"\")");
            preparedStatement.setString(1, username);
            preparedStatement.setString(2, sha256Hash(password));
            int count = preparedStatement.executeUpdate();
            if(count == 0){
                System.out.println("Register Failed, User Already Exist: " +username);
                throw new IllegalArgumentException("Username Already Exist: " + username);
            } else {
                System.out.println("Register Success: " + username);
                return true;
            }
        } catch (SQLException e){
            System.out.println("Register Failed, User Already Exist: " +username);
            throw new IllegalArgumentException("Username Already Exist: " + username);
        } finally {
            cleanResult();
        }
    }

    public String login(String username, String password) throws IllegalArgumentException{
        if(username == null || password == null || username.length()==0 || password.length()==0){
            System.err.println("Login Failed, username or password Illegal");
            throw new IllegalArgumentException("Login Failed, username or password Illegal");
        }
        try {
            preparedStatement = connect.prepareStatement("SELECT * FROM Users WHERE username = ? AND password = ?");
            preparedStatement.setString(1, username);
            preparedStatement.setString(2, sha256Hash(password));
            resultSet = preparedStatement.executeQuery();
            if(!resultSet.next()){
                System.out.println("No Such User: "+username);
                throw new IllegalArgumentException("Userword Password Doesn't match: " + username);
            } else {
                return resultSet.getString(3);
            }
        } catch (SQLException e){
            System.out.println("SQL Exception when login: " + e.toString());
            throw new IllegalArgumentException("SQL Exception: " + e.toString());
        } finally {
            cleanResult();
        }
    }

    // --------Following is method for front end tuning
    public List<IndexingItem> getWordUrls(String word){
        try {
            preparedStatement = connect.prepareStatement("SELECT *, ((LOG(2+tf)*6+headingScore+5*positionScore+" +String.valueOf(TITLE_IMPORTANCE)+ ")*((2*capitalPercent+2*titlePercent+5*(url like '%"+word+"%') " +
                    "+20*(url like'%"+word+".%')+2*emphasisPercent+2*metaPercent)+" +String.valueOf(TF_WEIGHT)+ ")) as scoreSum from Indexing where word = ? ORDER BY scoreSum DESC LIMIT "+String.valueOf(TOPK_PERWORD));
            preparedStatement.setString(1, word);
            resultSet = preparedStatement.executeQuery();
            List<IndexingItem> resultList = retreiveIndexingItem(resultSet);
            return resultList;
        } catch (SQLException e){
            System.err.println("Exception in getWordUrls: " + e.toString());
            e.printStackTrace();
            return null;
        } finally {
            cleanResult();
        }
    }

    public List<IndexingItem> getWordUrls(String word, String word2){
        System.out.println("url paris: " + word + " " + word2);
        try {
            preparedStatement = connect.prepareStatement("SELECT *, ((LOG(2+tf)*6+headingScore+5*positionScore+" +String.valueOf(TITLE_IMPORTANCE)+ ")*((2*capitalPercent+2*titlePercent+5*(url like '%"+word+"%') " +
                    "+20*(url like '%"+word2+"%') "+"+10*(url like'%"+word+".%')+2*emphasisPercent+2*metaPercent)+" +String.valueOf(TF_WEIGHT)+ ")) as scoreSum from Indexing where word = ? ORDER BY scoreSum DESC LIMIT "+String.valueOf(400));preparedStatement.setString(1, word);
            resultSet = preparedStatement.executeQuery();
            List<IndexingItem> resultList = retreiveIndexingItem(resultSet);
            return resultList;
        } catch (SQLException e){
            System.err.println("Exception in getWordUrls: " + e.toString());
            e.printStackTrace();
            return null;
        } finally {
            cleanResult();
        }

    }

    public List<IndexingItem> getWordUrlsCache(String word){
        try {
            preparedStatement = connect.prepareStatement("SELECT * from IndexingCache where word = ? ORDER BY score DESC LIMIT "+String.valueOf(TOPK_PERWORD));
            preparedStatement.setString(1, word);
            resultSet = preparedStatement.executeQuery();
            List<IndexingItem> resultList = retrieveIndexingItemCache(resultSet);
            return resultList;
        } catch (SQLException e){
            System.err.println("Exception in getWordUrls: " + e.toString());
            e.printStackTrace();
            return null;
        } finally {
            cleanResult();
        }
    }

    public List<IndexingItem> getWordUrlsCache(String word, String word2){
        System.out.println("url paris: " + word + " " + word2);
        try {
            preparedStatement = connect.prepareStatement("SELECT * from IndexingCache where word = ? ORDER BY score DESC LIMIT "+String.valueOf(600));
            preparedStatement.setString(1, word);
            resultSet = preparedStatement.executeQuery();
            List<IndexingItem> resultList = retrieveIndexingItemCache(resultSet);
            return resultList;
        } catch (SQLException e){
            System.err.println("Exception in getWordUrls: " + e.toString());
            e.printStackTrace();
            return null;
        } finally {
            cleanResult();
        }

    }

    public float getPageRank(String url){
        String host = getHost(url);
        if(pageRankMap.containsKey(host))
            return pageRankMap.get(host);
        else
            return 0.15f;
//        try{
//            String host = getHost(url);
//            preparedStatement = connect.prepareStatement("SELECT rank from PageRank where url = ?");
//            preparedStatement.setString(1, host);
//            resultSet = preparedStatement.executeQuery();
//            if(!resultSet.next()){
//                System.out.println("No Such Host: "+host);
//                return 0.15f;
//            } else {
//                return resultSet.getFloat(1);
//            }
//        } catch (SQLException e){
//            e.printStackTrace();
//            return 0.15f;
//        } finally {
//            cleanResult();
//        }
    }

    public List<UrlResult> evaluateQuery(String query, String profile){
        String[] words = query.split(" ");
        HashMap<String, UrlResult> map = new HashMap<>();
        System.out.println("T1:" + System.currentTimeMillis());
        for(String word : words){
            List<IndexingItem> listItems = getWordUrls(word);
            //List<IndexingItem> listItems = getWordUrlsCache(word);
            System.out.println("T:" + word +" " + System.currentTimeMillis());
            for(IndexingItem item : listItems){
                String url = item.url;
                float scoreSum = item.scoreSum;
                UrlResult urlResult = map.get(url);
                if(urlResult == null){
                    float pageRank = getPageRank(url);
                    urlResult = new UrlResult(0, url, pageRank);
                    map.put(url, urlResult);
                }
                urlResult.score += scoreSum;
                urlResult.putWord(item);
            }
        }
        System.out.println("T2:" + System.currentTimeMillis());
        for(int i = 0; i < words.length; i++){
            for(int j = 0; j < words.length; j++){
                if(i==j)continue;
                System.out.println("T3:" + i + " " + j + " " + System.currentTimeMillis());
                List<IndexingItem> listItems = getWordUrls(words[i],words[j]);
                //List<IndexingItem> listItems = getWordUrlsCache(words[i],words[j]);
                for(IndexingItem item : listItems){
                    String url = item.url;
                    float scoreSum = item.scoreSum;
                    UrlResult urlResult = map.get(url);
                    if(urlResult == null){
                        float pageRank = getPageRank(url);
                        urlResult = new UrlResult(0, url, pageRank);
                        map.put(url, urlResult);
                    }
                    urlResult.score += scoreSum;
                }
            }
        }
        System.out.println("T4:" + System.currentTimeMillis());
        ArrayList<UrlResult> result = new ArrayList<UrlResult>();
        result.addAll(map.values());
        Set<String> historySet = new HashSet<String>();
        if(profile!=null && profile.length()!=0){
            for(String s : profile.split(",")){
                historySet.add(getHost(s));
            }
        }
        for(UrlResult r : result){
            r.getNeighborScore();
            if(historySet.contains(getHost(r.url)))
                r.score = r.score * USER_PROFILE_WEIGHT;
            r.score = r.score + r.score * r.neighborScore * WORD_NEIGHBOR_WEIGHT;
            r.score = r.score + r.score * (float)Math.log(2+r.pageRank) * PAGE_RANK_WEIGHT;
        }
        System.out.println("T5:" + System.currentTimeMillis());
        sort(result);
        System.out.println("T6:" + System.currentTimeMillis());
        return result.subList(0, RESULT_NUMBER < result.size()? RESULT_NUMBER : result.size());
    }

    // Methods for proving user specific content
    public void updateProfile(String username, String profile){
        if(username == null || profile == null || username.length()==0 || profile.length()==0){
            System.err.println("Click Failed, username or profile Illegal");
            throw new IllegalArgumentException("Click Failed, username or profile Illegal");
        }
        try {
            preparedStatement = connect.prepareStatement("SELECT * FROM Users WHERE username = ?");
            preparedStatement.setString(1, username);
            resultSet = preparedStatement.executeQuery();
            if(!resultSet.next()){
                System.out.println("No Such User: "+username);
                throw new IllegalArgumentException("Username Doesn't exist: " + username);
            } else {
                if(profile.length() < 2048) {
                    statement = connect.createStatement();
                    statement.executeUpdate("UPDATE Users SET Users.profile = '"+profile+"' Where Users.username='"+username+"'");
                }
            }
        } catch (SQLException e){
            System.out.println("SQL Exception when login: " + e.toString());
            throw new IllegalArgumentException("SQL Exception: " + e.toString());
        } finally {
            cleanResult();
        }
    }


    // This Main Method acts as a test case and example of how to use this wrapper
    public static void main(String[] args) throws Exception{
        MySQLWrapper db = new MySQLWrapper();
//        System.out.println(db.sha256Hash("yhg123"));
//        System.out.println(db.sha256Hash("yhg123").length());
//        try {
//            System.out.println(db.login("u1", "p1"));
//        } catch (IllegalArgumentException e){
//            System.out.println(e.toString());
//        }
//        db.register("u1","p1");
//        System.out.println(db.login("u1","p1"));
//        try {
//            db.register("u1", "p2");
//        } catch (IllegalArgumentException e){
//            System.out.println(e.toString());
//        }
//        for(IndexingItem item :db.getWordUrls("Perceptron")){
//            System.out.println(item.word + " " + item.url + " " + item.scoreSum);
//        }
//        for(UrlResult result : db.evaluateQuery("septa philadelphia transit", null)){
//            System.out.println(result.toString());
//        }

        db.close();
    }

    //
}
