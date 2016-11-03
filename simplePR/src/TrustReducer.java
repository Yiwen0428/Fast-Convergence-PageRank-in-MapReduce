import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class TrustReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
         	
	double d = 0.85;
	int N = 685230;
	double prank = 0.0;
    String[] node = null;
	for(Text text : values) {
	   String line = text.toString();
	   line = line.trim();
       if (line.contains("@")) {
    	   line = line.trim();
    	   line = line.replace("@", "");
    	   node = line.split("\\s+");
       }
       else
    	   prank = prank + Double.parseDouble(line);
    }
	prank = d * prank  + (1-d) / N;
	
	if (node.length > 3) {
		double p1 = Double.parseDouble(node[2]);
		node[2] = Double.toString(prank);
		double re = Math.abs(p1 - prank) / prank;
		re = re * Math.pow(10, 5);
		context.getCounter(My_counter.counter.residual).increment((long)re);
		String v_S = node[1] + " " + node[2] + " " + node[3];
		Text v = new Text(v_S);
	
		context.write(key, v);
	}
	else {
		double p1 = Double.parseDouble(node[2]);
		node[2] = Double.toString(prank + p1);
		double re = Math.abs(prank) / (prank + p1);
		re = re * Math.pow(10, 5);
		context.getCounter(My_counter.counter.residual).increment((long)re);
		String v = node[1] + " " + node[2];
		context.write(key, new Text(v));
	}
    }
}
