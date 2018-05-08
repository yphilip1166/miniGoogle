package edu.upenn.cis455.crawler;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public class Monitor {
	InetAddress host;
	DatagramSocket s;
	
	public Monitor(String name) throws UnknownHostException, SocketException{
		host = InetAddress.getByName(name); 
		System.out.println("Got hostname:"+host);
		s = new DatagramSocket();
	}
	
	// send monitor info
	public void send(String url) throws IOException{
		byte[] data = ("yecheng;"+url).getBytes();
		DatagramPacket packet = new DatagramPacket(data, data.length, host, 10455);
		s.send(packet);
	}
}
