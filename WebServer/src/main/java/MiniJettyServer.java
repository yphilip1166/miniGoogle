
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.webapp.WebAppContext;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Scanner;

public class MiniJettyServer  {
    public static MySQLWrapper mysql;
    public static HashSet<String> stopWords;
    public static void main(String[] args) throws Exception {

        Server server = new Server(8080);
        mysql = new MySQLWrapper();
        stopWords = new HashSet<>();

        WebAppContext context = new WebAppContext();
        String path = System.getProperty("user.dir");

        //String digest = DigestUtils.sha1Hex("http://notquitetheme.tumblr.com");
        //System.out.println("test: "+digest);
        File f = new File(path + "/public/doc/" + "stopwords.txt");
        StringBuilder sb = new StringBuilder();
        Scanner sc = null;
        try{
            sc = new Scanner(f);
        } catch(FileNotFoundException fe){
            System.out.println("stopwords.txt not found");
        }


        while (sc.hasNext()){
            String word = sc.nextLine();
            word = word.split("/n")[0].toLowerCase();
            stopWords.add(word);
        }

        context.setDescriptor("conf/web.xml");
        context.setContextPath("/");
        context.setResourceBase(path + "/public/");
        context.setParentLoaderPriority(false);
        server.setHandler(context);

        server.start();
        server.join();


    }
}
