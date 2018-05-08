import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class RetrieveGraph {
    public static String getHost(String urlString) {
        try {
            URL url = new URL(urlString);
            return url.getHost();
        } catch (MalformedURLException e) {
            System.err.println("Malformed url:" + e.toString());
            return null;
        }
    }
    public static PrintWriter pw;
    public static void readFile(String filePath){
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line = br.readLine();
            if(line ==null)return;
            String[] part = line.split(",");
            if (part.length != 2) return;
            String fromUrl = part[0];
            String toUrl = part[1];
            pw.write(getHost(fromUrl) + "," + getHost(toUrl) + "\n");
            br.close();
        } catch (Exception e){
            System.err.println("Error process: " + filePath + " " + e.toString());
        }
    }
    public static void main(String[] args){
        String inputDir="/Users/hugangyu/Documents/2017Fall/CIS555 Internet and Web/project/s3data/crawlerOutput/";
        String outputFile = "data/graph_large.csv";
        int i = 0;
        try {
            File dir = new File(inputDir);
            File[] filesList = dir.listFiles();
            for (File file : filesList) {
                if (file.isFile()) {
                    if(i%100000 == 0) {
                        if (pw != null)
                            pw.close();
                        pw = new PrintWriter("graphCombined/combinedGraph" + String.valueOf(i / 100000) + ".csv", "UTF-8");
                    }
                    readFile(inputDir + "/" + file.getName());
                    i++;
                }
            }
            if(pw != null) pw.close();
        } catch (Exception e){
            System.err.println("Error process: " + inputDir + " " + e.toString());
            e.printStackTrace();
        }
    }
}

