package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Created by Carlton on 4/30/18.
 */
public class PageRankUpdator extends Thread{

    private static Queue<String> updateQueue = new ArrayDeque<>();
    private boolean running = true;

    public static void addDependency(String from, String to){
        if (from !=null && to != null) {
            updateQueue.offer(from+","+to);
        }
    }

    @Override
    public void run() {

        File file = null;
        PrintWriter pw = null;
        int counter = 0;
        try {
            file = new File("dependency.txt");
            if (!file.exists()) {
                file.createNewFile();
            }
            pw = new PrintWriter(new FileOutputStream(file, true));

            while (running) {
                if (updateQueue.isEmpty()) {
                    Thread.sleep(10000);
                }
                if (!updateQueue.isEmpty()){
                    String toAdd = updateQueue.poll();
                    if (toAdd != null){
                        pw.println(toAdd);
                    }
                    if (++counter == 10){
                        counter = 0;
                        pw.flush();
                    }

                }


            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (pw != null) {
                pw.close();
            }
        }
    }
}
