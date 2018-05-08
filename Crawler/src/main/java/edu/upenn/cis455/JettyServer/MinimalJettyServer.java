package edu.upenn.cis455.JettyServer;

/**
 * Created by CIS555 Team on 3/10/17.
 */
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

public class MinimalJettyServer  {
    public static void main(String[] args) throws Exception {
        Server server = new Server(8080);

        WebAppContext context = new WebAppContext();

        context.setDescriptor("conf/web.xml");
        context.setContextPath("/");
        context.setResourceBase(".");
        context.setParentLoaderPriority(false);
        server.setHandler(context);

        server.start();
        server.join();
    }
}
