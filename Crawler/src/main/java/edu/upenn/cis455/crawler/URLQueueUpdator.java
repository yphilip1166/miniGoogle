package edu.upenn.cis455.crawler;

/**
 * Created by Carlton on 4/23/18.
 */
public class URLQueueUpdator extends Thread{

    private boolean running = true;
    private static final int duration = 60000;

    @Override
    public void run(){
        while (running) {
            try {
                XPathCrawlerInfo.storeUrlToDisk();
            } catch (Exception e) {
                //e.printStackTrace();
            } finally {
                try {
                    Thread.sleep(duration);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
