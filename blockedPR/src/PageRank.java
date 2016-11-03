import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;


public class PageRank {

    public static void main(String[] args) throws IOException {
        boolean flag = true;
        int num_pass = 0;
        while(flag) {
        	Configuration conf = new Configuration();
            Job job =new Job(conf);
            
        
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);
            
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setJarByClass(PageRank.class);
            
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setMapperClass(TrustMapper.class);
            job.setReducerClass(TrustReducer.class);
            
            String inputPath = null;
            if (num_pass == 0) {
            
            	inputPath = args[0];
                if (inputPath == null) {
                    System.err.println("No input file!");
                    return;
                }
            } 
            else {
            	inputPath = args[1]+"/pass" + (num_pass-1);

            }
            
            String outputPath = args[1]+"/pass" + num_pass;

            
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            try { 
                job.waitForCompletion(true);
            } catch(Exception e) {
                System.err.println(e);
                return;
            }          
            long residual = job.getCounters().findCounter(My_counter.counter.residual).getValue();
            double ave_residual = ((double)(residual * Math.pow(10, -5)))/685230;
            long loops = job.getCounters().findCounter(My_counter.counter.loop).getValue();
            double ave_loop = loops/68.0;
            System.out.println("pass" + " " + num_pass + " avg error " + Double.toString(ave_residual));
            //out.write("pass" + " " + num_pass + " avg error " + Double.toString(ave_residual));
           // out.newLine();
            System.out.println("Total loops for pass" + " " + num_pass + " is " + Long.toString(loops));
           // out.write("Total loops for pass" + " " + num_pass + " is " + Long.toString(loops));
            //out.newLine();
            System.out.println("avg loops for pass" + " " + num_pass + " is " + Double.toString(ave_loop));
            //out.write("avg loops for pass" + " " + num_pass + " is " + Double.toString(ave_loop));
            //out.newLine();
            if (ave_residual < 0.001) flag = false;
            num_pass = num_pass + 1;
        }
       // out.close();
    }

}
