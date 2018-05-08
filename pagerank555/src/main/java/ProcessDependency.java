import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class ProcessDependency {
    public static String getHost(String urlString){
        try {
            URL url = new URL(urlString);
            return url.getHost();
        } catch (MalformedURLException e){
            System.err.println("Malformed url:" + e.toString());
            return null;
        }
    }
    //public static Set<String> set;
    public static PrintWriter pw;
    public static void readFile(String filePath){
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));

            while (true) {
                String line = br.readLine();
                if (line == null) break;
                pw.write(line+"\n");
                //set.add(getHost(fromUrl) + "," + getHost(toUrl) + "\n");
            }

//            for (String s : set) {
//                pw.write(s);
//            }
        } catch (Exception e){
            System.err.println("Error process: " + filePath + " " + e.toString());
        }
    }
    public static void main(String[] args){
        String outputFile = "data/graph2.csv";
        //set = new HashSet<String>();
        try {
            pw = new PrintWriter(outputFile,"UTF-8");
            readFile("graphCombined/combinedGraph0.csv");
            readFile("graphCombined/combinedGraph1.csv");
            readFile("graphCombined/combinedGraph2.csv");
            readFile("graphCombined/combinedGraph3.csv");
            readFile("graphCombined/combinedGraph4.csv");
            readFile("graphCombined/combinedGraph5.csv");
            //readFile("data/dependency2.txt");
            pw.close();
        } catch (Exception e){
            System.err.println("Error process: " + e.toString());
            e.printStackTrace();
        }
    }
}
