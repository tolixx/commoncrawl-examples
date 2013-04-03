package org.commoncrawl.examples;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;


class IntegerPair implements Writable {
	protected Integer first = 0;
	protected Integer second = 0;

	public IntegerPair() {

	}

	public IntegerPair ( Integer first, Integer second ) {
		init ( first, second ); 
	}


	public void init ( Integer first, Integer second ) {
		this.first = first;
		this.second = second;
	}


	public void write(DataOutput out) throws IOException {
		   out.writeUTF(this.first.toString() + "\t" + this.second.toString());
    }

    public String toString () {
	   StringBuffer sb = new StringBuffer();

	   sb.append(this.first.toString());
	   sb.append("\t"+this.second.toString());
	   
	   return(sb.toString());
	   
    }

    public void readFields(DataInput in) throws IOException {
	   String  rawData = in.readUTF();
       String[] data = rawData.split("\t");

       this.first  = Integer.valueOf(data[0]);
       this.second = Integer.valueOf(data[1]); 
	  
    }

    public Integer first() {
    	return first;
    } 

    public Integer second() {
    	return second; 
    }

}