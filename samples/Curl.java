/**
 * 
 */
package com.amazonaws.samples;

import java.io.IOException; 
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.Calendar; 

public class Curl{ 
    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException{ 
	    try { 
           Process ls = Runtime.getRuntime().exec(new String[]{"mkdir", "vanhai"}); 
        } catch (IOException e1) { 
            e1.printStackTrace();   
            System.out.print("erorr");
        }        
	   String date_time = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
	   System.out.println(date_time);
       PrintWriter writer = new PrintWriter("./vanhai/"+date_time+".txt", "UTF-8");
       writer.println("The first line");
       writer.println("The second line");
       writer.close();
    }   
}  