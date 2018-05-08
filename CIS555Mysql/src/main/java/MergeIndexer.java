import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.HashSet;

public class MergeIndexer {
    public static PrintWriter pw;

    public static void readFile(String filePath){
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));

            while (true) {
                String line = br.readLine();
                if (line == null) break;
                pw.write(line+"\n");
            }
        } catch (Exception e){
            System.err.println("Error process: " + filePath + " " + e.toString());
        }
    }

    public static void main(String[] args){
        String inputDir="/Users/hugangyu/Documents/2017Fall/CIS555 Internet and Web/project/s3data/indexerOutput/";
        String outputFile = "IndexingData/IndexerCombined/combinedIndex.txt";
        int i = 0;
        try {

            File dir = new File(inputDir);
            File[] filesList = dir.listFiles();
            for (File file : filesList) {
                if (file.isFile()) {
                    if(i%10 == 0) {
                        if (pw != null)
                            pw.close();
                        pw = new PrintWriter("combinedIndexNew" + String.valueOf(i / 10) + ".csv", "UTF-8");
                    }
                    System.out.println(inputDir + "/" + file.getName());
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
