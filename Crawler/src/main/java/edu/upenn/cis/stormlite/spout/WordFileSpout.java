package edu.upenn.cis.stormlite.spout;

public class WordFileSpout extends FileSpout {

		// a class that extends FileSpout so that it can be instantiated
	@Override
	public String getFilename() {
		return "words.txt";
	}

}
