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
        //Implement
    final double d = 0.85;
    final int N = 685230;

    String[] node = null;
    ArrayList<Long> nodes = new ArrayList<Long>();
    HashMap<Long, ArrayList<Double>> hm_c = new HashMap<Long, ArrayList<Double>>();
    HashMap<Long, ArrayList<Long>> hm_e = new HashMap<Long, ArrayList<Long>>();
    HashMap<Long, String> init = new HashMap<Long, String>();
    HashMap<Long, Double> old = new HashMap<Long, Double>();
    HashMap<Long, Double> nev = new HashMap<Long, Double>();
    
    for (Text text: values) {
    	String line = text.toString();
    	line = line.trim();
    	if (line.contains("@")) {
     	   line = line.trim();
     	   line = line.replace("@", "");
     	   node = line.split("\\s+");
     	   long id = Long.parseLong(node[1]);
     	   init.put(id, line);
     	   nodes.add(id);
     
     	   old.put(id, Double.parseDouble(node[2]));
     	  
     	}
    	
    	else if (line.contains("c")) {
    		line = line.trim();
    		line = line.replace("c", "");
    		node = line.split("/");
    		long  v = Long.parseLong(node[1]);
    		if (!hm_c.containsKey(v)) {
    			ArrayList<Double> R = new ArrayList<Double>();
    			R.add(Double.parseDouble(node[2]));
    			hm_c.put(v, R);
    		}
    		else {
    			hm_c.get(v).add(Double.parseDouble(node[2]));
    		}
    	}
    
    	else if (line.contains("e")) {
    		line = line.trim();
    		line = line.replace("e", "");
    		node = line.split("/");
    		long v = Long.parseLong(node[1]);
    		long u = Long.parseLong(node[0]);
    		if (!hm_e.containsKey(v)) {
    			ArrayList<Long> U = new ArrayList<Long>();
    			U.add(u);
    			hm_e.put(v, U);
    		}
    		else {
    			hm_e.get(v).add(u);
    		}
    	}
    }
    
    boolean flag = true;
    int i=0;
	while (flag) {
		double residual = 0.0;
		for (int j = 0; j < nodes.size(); j++) {
			long b_v = nodes.get(j);
			double NPR = 0.0;
			double PR = old.get(b_v);
			
			if (hm_c.containsKey(b_v)) {
				ArrayList<Double> tpr = hm_c.get(b_v);
				for (int k = 0; k < tpr.size(); k++) {
					NPR += tpr.get(k);
				}
			}
			if (hm_e.containsKey(b_v)) {
				ArrayList<Long> uv = hm_e.get(b_v);
				for (int k = 0; k < uv.size(); k++) {
					String[] uv_s = init.get(uv.get(k)).split("\\s+");
					double npr = old.get(uv.get(k))/uv_s[3].split(",").length;
					NPR += npr;
				}	
			}
			
			NPR = d * NPR + (1-d)/N;
			residual = residual + Math.abs(NPR - PR) / NPR;
			nev.put(b_v, NPR);
		}
	
		double av_re = residual / nodes.size();
		if (av_re < 0.001 || i == 20) flag = false;
		
		for (int h = 0; h < nodes.size(); h++) {
			long b_v = nodes.get(h);
			double tpr = nev.get(b_v);
			old.put(b_v, tpr);
		}
		i++;
		
	}
	
	context.getCounter(My_counter.counter.loop).increment((long)i);
	
	for (int j = 0; j < nodes.size(); j++) {
		
		double end = old.get(nodes.get(j));
		String start = init.get(nodes.get(j));
		
		String[] s = start.split("\\s+");

		double prs = Double.parseDouble(s[2]);
		
		double re = Math.abs(prs-end) / end;
		re = re * Math.pow(10, 5);
		String info = "";
		if (s.length > 3) {
			info = s[1] + " "+ end + " " + s[3];
		}
		else {
			info = s[1] + " " + end;
		}
		
		context.getCounter(My_counter.counter.residual).increment((long)re);
		context.write(new LongWritable(nodes.get(i)), new Text(info));
	}
  }
}
