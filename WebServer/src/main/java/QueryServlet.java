import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import javax.servlet.http.HttpSession;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;

public class QueryServlet extends HttpServlet {
    public MySQLWrapper mysql;
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        HttpSession s = request.getSession(false);
        if(s == null){
            response.sendRedirect("/home");
        }
        System.out.println("request query is"+request.getRequestURI());
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException{
        HttpSession s = request.getSession(false);
        if(s == null){
            response.sendRedirect("/home");
        }
        //System.out.println("Receive Post" + request.getRequestURI());
        String query = request.getParameter("queryName");

        if(query == null || query.trim().length()==0){
            System.out.println("empty query");
            response.sendRedirect("/home");
            return;
        }

        String processed = processStopWords(query);

        if(processed.length() == 0){
            System.out.println("empty query");
            response.sendRedirect("/home");
            return;
        }

        System.out.println("Query is: "+ query);
        HttpSession session = request.getSession();
        String username=null;
        String profile = null;
        if(session==null){
            System.out.println("Internal Error, Query null session");
            response.sendRedirect("/home/login");
            return;
        }
        profile = (String)session.getAttribute("profile");
        List<UrlResult> results = mysql.evaluateQuery(query, profile);
        session.setAttribute("queryResults",results);

        for(UrlResult result: results){
            System.out.println(result.toString()+"\n");
        }

        response.sendRedirect("/result?startIdx=0");
    }
    public void init(){
        System.out.println("Init from QueryServlet");
        mysql = MiniJettyServer.mysql;
    }

    private String processStopWords(String query) {
        int count=0;
        String[] words = query.split(" ");

        if(!query.toUpperCase().equals(query)){
            query = query.toLowerCase();
        }

        String original = new String(query);

        for(String word: words){
            if(MiniJettyServer.stopWords.contains(word)){
                query.replace(word, "");
                count++;
            }
        }
        if(count > query.length()+1){
            return query;
        }else{
            return original;
        }
    }
}
