import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;
import java.net.URL;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ResultServlet extends HttpServlet {
    MySQLWrapper mysql;
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        HttpSession s = request.getSession(false);
        if(s == null){
            response.sendRedirect("/home");
        }
        String uri = request.getRequestURI().toString();
        if(uri.contains("result/")){

            String profile = (String)s.getAttribute("profile");
            String url = uri.substring(8, uri.length());
            if(profile == null){
                profile = new String("");
            }
            if(profile.trim().length() == 0){
                profile = url;
            }else{
                profile = profile + "," + url;
            }
            System.out.println("profile"+profile);
            s.setAttribute("profile", profile);
            String username = (String)s.getAttribute("username");
            MiniJettyServer.mysql.updateProfile(username, profile);

            response.sendRedirect(url);
            return;
        }


        String startIdxStr = request.getParameter("startIdx");
        int startIdx = Integer.parseInt(startIdxStr);
        System.out.println(startIdx);
        MySQLWrapper.getHost("https://www.google.com/a/f/f");

        List<UrlResult> results = (List<UrlResult>)s.getAttribute("queryResults");
        int resultsSize = results.size();
        int maxPage = resultsSize/10 + 1;

        PrintWriter pw = response.getWriter();

        String path = System.getProperty("user.dir");
        File f = new File(path + "/public/html/" + "results.html");
        StringBuilder sb = new StringBuilder();
        Scanner sc = new Scanner(f);

        int pageCount = 0;
        int titleCount = 0;
        UrlResult result = null;
        String url = null;
        while (sc.hasNext()){
            String line = sc.nextLine();
            if(line.contains("titleholder")){
                if(titleCount+startIdx < results.size()){
                    int idx = startIdx + titleCount++;
                    result = results.get(idx);
                    url = result.url;
//                System.out.println(url);
//                Document doc = Jsoup.connect(url).get();
//                String title = doc.title();

                    String title = new URL(result.url).getHost();
                    if(title.startsWith("www")){
                        System.out.println(title);
                        title = title.split("\\.")[1];
                    }else{
                        title = title.split("\\.")[0];
                    }
                    title = title + " :";

                    for(IndexingItem word: result.relatedWord){
                        title = title + " " + word.word;
                    }
                    line = line.replace("titleholder", title);
                    line = line.replace("\"\"", "/result/"+url);
                }else{
                    line = line.replace("titleholder", "");
                }
            }
            if(line.contains("urlholder")){
                if(titleCount+startIdx < results.size()){
                    line = line.replace("urlholder", url);
                }else{
                    line = line.replace("urlholder", "");
                }
            }
            if(line.contains("disholder") && titleCount+startIdx < results.size()){
                if(titleCount+startIdx < results.size()){
                    float score = result.score;
                    float pageRank = result.pageRank;
                    float neighborScore = result.getNeighborScore();
                    String debugInfo = "Total Score: " + score + "\t"
                            + "Page Rank: "+ pageRank + "\t"
                            + "Neighbor Score: "+ neighborScore;
                    line = line.replace("disholder", debugInfo);
                }else {
                    line = line.replace("disholder", " ");
                }
            }
            if(line.contains("page-item")){
                if(startIdx/10 == pageCount || pageCount > maxPage){
                    line = line.replace("page-item", "page-item disabled");
                }
                pageCount++;
            }
            sb.append(line + "\n");
        }
        sc.close();
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("text/html");

        pw.println(sb.toString());
        pw.flush();
    }
    public void doPost(HttpServletRequest request, HttpServletResponse response) {

    }

    public void init(){
        mysql = MiniJettyServer.mysql;
    }
}
