import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class TrustMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	long cur_blockid = 0;
    	long blockid = 0;
    	String value_s = value.toString();
    	String[] line = value_s.split("\\s+");
 	    long nid = Long.parseLong(line[1]);
 	    cur_blockid = blockIDofNode(nid);
 	   
    	if (line.length > 3) {	
        	double prank = Double.parseDouble(line[2]);
    		String[] outlink = line[3].split(",");
    		double outpr = prank/outlink.length;
    		
    		for (int i = 0; i < outlink.length; i++) {
    			long neighbor = Long.parseLong(outlink[i]);
    			long block_n = blockIDofNode(neighbor);
    			if (cur_blockid != block_n) {
    				String info1 = "c" + Long.toString(nid)+ "/" + Long.toString(neighbor) + "/"
    						 + Double.toString(outpr);
    				context.write(new LongWritable(block_n), new Text(info1));
    			}
    			
    			else {
    				String info2 =  "e" + Long.toString(nid) + "/" + Long.toString(neighbor);
    				context.write(new LongWritable(block_n), new Text(info2));
    			}
    		}		
    	}
    	String info2 = "@" + value_s;
        Text v2 = new Text(info2);
        context.write(new LongWritable(cur_blockid), v2);
    	
    }
    
    public long blockIDofNode(long nodeID) throws IOException {
    	long blockid = 0;
    	long t = new Long(nodeID).hashCode();
    	blockid = t % 68;
    	return blockid;
   }
}
