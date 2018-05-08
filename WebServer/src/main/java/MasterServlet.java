import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class MasterServlet extends HttpServlet{
    private DBWrapper db;
    public MySQLWrapper mysql;
    public void init(){
        System.out.println("Servlet Init");
        mysql = MiniJettyServer.mysql;
    }
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String path = System.getProperty("user.dir");
        String dir = getServletContext().getInitParameter("BDBstore");
        PrintWriter pw = response.getWriter();

        File directory = new File(path);
        if (!directory.exists()) {
            directory.mkdirs();
        }

        db = DBWrapper.getInstance(dir);

        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);

        PrintWriter out;
        try {
            out = response.getWriter();
        } catch (IOException e) {
            db.shutdown();
            e.printStackTrace();
            return;
        }

        String uri = request.getRequestURI().toString();
        HttpSession session;

        // /register
        if(uri.endsWith("register")){
            handleRegister(out, null);
            return;
        }

        if (uri.endsWith("logout")) {

            session = request.getSession(false);

            if(session == null){
                response.sendRedirect("/home");
                return;
            }
            String username = (String)session.getAttribute("username");
            String profile = (String)session.getAttribute("profile");
            mysql.updateProfile(username, profile);


            session.invalidate();
            handleLogin(out, null);
            return;
        } else {
            session = request.getSession(false);
        }

        String username;
        User user = null;
        //Retrieve logged in user information
        if (session != null) {
            username = (String) session.getAttribute("username");
            generateHome(pw, path, username);
        } else{
            handleLogin(out, null);
        }


    }

    public void doPost(HttpServletRequest request, HttpServletResponse response) {
        String path = System.getProperty("user.dir");
        String uri = request.getRequestURI().toString();
        response.setContentType("text/html");
        System.out.println("Receive Post" + request.getRequestURI());

        PrintWriter out;
        try {
            out = response.getWriter();
        } catch (IOException e) {
            db.shutdown();
            e.printStackTrace();
            return;
        }

        if(uri.equals("/query")){
            String query = request.getParameter("queryName");
            System.out.println("Query is: "+ query);
            HttpSession session = request.getSession(false);
            String username=null;
            String profile = null;
            if(session==null){
                System.out.println("Internal Error, Query null session");
                handleLogin(out, "Internal Error, Query null session");
                return;
            }
            profile = (String)session.getAttribute("profile");
            List<UrlResult> results = mysql.evaluateQuery(query, profile);
            session.setAttribute("queryResults",results);

        }
        if(uri.endsWith("register")){
            String username = request.getParameter("username");
            String password = request.getParameter("password");

            if(username == null || username.trim().length() == 0){
                handleRegister(out, "Username could not be empty. Please try again.");
                //db.shutdown();
                return;
            }
            if(password == null || password.trim().length() < 6){
                handleRegister(out, "Password must have as least six chatacters. Please try again.");
                //db.shutdown();
                return;
            }
            try{
                mysql.register(username, password);
                handleLogin(out, null);
            } catch (IllegalArgumentException e){
                System.out.println("Register Failed: " + e.toString());
                handleRegister(out, "Register Failed in DB: " + e.toString());
            }
//            if(db.containsUser(username)){
//                handleRegister(out, "Username exists. Please try again.");
//                //db.shutdown();
//                return;
//            }
//            try {
//                db.storeUser(username, password);
//                handleLogin(out, null);
//                //db.shutdown();
//                return;
//            } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
//                handleRegister(out, "Invlid password format. Please try again.");
//                //db.shutdown();
//                return;
//            }
        }

        if(uri.endsWith("login")){
            String username = request.getParameter("username");
            String password = request.getParameter("password");

            if(username == null || username.trim().length() == 0){
                handleLogin(out, "Username could not be empty. Please try again.");
                //db.shutdown();
                return;
            }
            if(password == null){
                handleLogin(out, "Please enter passowrd.");
                //db.shutdown();
                return;
            }
//            if(!db.containsUser(username)){
//                handleLogin(out, "Username does not exist.");
//                //db.shutdown();
//                return;
//            }
            try {
//                if(db.checkCombination(username, password) == false){
//                    handleLogin(out, "Combination is incorrect. Please try again.");
//                    //db.shutdown();
//                    return;
//                }
//
//                HttpSession session = request.getSession(true);
//                session.setAttribute("username", username);
//                User user = db.getUser(username);
//                try{
//                    generateHome(out, path, user.getUsername());
//                }catch (FileNotFoundException fe){
//                    System.out.println("home.html not found");
//                }

                //db.shutdown();
                // TODO: save profile
                String profile = mysql.login(username, password);
                HttpSession session = request.getSession(true);
                session.setAttribute("username", username);
                session.setAttribute("profile", profile);
                generateHome(out, path, username);
                return;

            } catch (IllegalArgumentException e) {
                handleLogin(out, e.toString());
                //db.shutdown();
                return;
            } catch (FileNotFoundException e){
                System.out.println("Html File Not Found: Internal Error");
            }

        }
    }

    private void generateHome(PrintWriter pw, String path, String msg) throws FileNotFoundException {
        File f = new File(path + "/public/html/" + "home.html");
        StringBuilder sb = new StringBuilder();
        Scanner sc = new Scanner(f);
        while (sc.hasNext()) {
            String line = sc.nextLine();
            if(line.contains("UserPlaceHolder")){
                System.out.println(line);
                String greeting = checkTime() + msg;
                line = line.replace("UserPlaceHolder", greeting);
            }
            sb.append(line + "\n");
        }
        sc.close();
        pw.println(sb.toString());
        pw.flush();
        pw.close();
    }


    private void handleLogin(PrintWriter out, String msg){
        String path = System.getProperty("user.dir");
        File f = new File(path + "/public/html/" + "login.html");
        StringBuilder sb = new StringBuilder();
        Scanner sc = null;
        try{
            sc = new Scanner(f);
        } catch (FileNotFoundException fe){
            System.out.println("login.html not found");
            return;
        }


        while (sc.hasNext()){
            String line = sc.nextLine();
            if(line.contains("h4") && msg != null){
                line = line.replace("</h4>", msg+"</h4>");
            }
            sb.append(line + "\n");
        }

        sc.close();
        out.println(sb.toString());
        out.close();
    }

    private void handleRegister(PrintWriter out, String msg){
        String path = System.getProperty("user.dir");
        File f = new File(path + "/public/html/" + "register.html");
        StringBuilder sb = new StringBuilder();
        Scanner sc = null;
        try{
            sc = new Scanner(f);
        } catch (FileNotFoundException fe){
            System.out.println("register.html not found");
            return;
        }

        while (sc.hasNext()){
            String line = sc.nextLine();
            if(line.contains("h4") && msg != null){
                line = line.replace("</h4>", msg+"</h4>");
            }
            sb.append(line + "\n");
        }

        sc.close();
        out.println(sb.toString());
        out.close();
    }

    private String checkTime(){
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
        SimpleDateFormat df = new SimpleDateFormat("kk:mm:ss");
        df.setTimeZone(cal.getTimeZone());
        String time = df.format(cal.getTime());
        System.out.println(time);
        String hourStr = time.split(":")[0];
        System.out.println(hourStr);
        int hour = Integer.parseInt(hourStr);
        if(hour >= 6 && hour < 12){
            return "Good Morning, ";
        }
        else if(hour >=12 && hour < 17){
            return "Good Afternoon, ";
        }
        else if(hour >=17 && hour < 21){
            return "Good Evening, ";
        }
        else{
            return "Good Night, ";
        }
    }
}
