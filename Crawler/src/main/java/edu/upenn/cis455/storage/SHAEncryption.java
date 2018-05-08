package edu.upenn.cis455.storage;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHAEncryption {
	public static String encode(String toEncode) throws NoSuchAlgorithmException{
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		byte[] encodedAr =  digest.digest(toEncode.getBytes(StandardCharsets.UTF_8));

		String result = null;
        try {
            result = new String(encodedAr, "ISO-8859-1");
        } catch (UnsupportedEncodingException e){

        }
        return result;
	}
}
