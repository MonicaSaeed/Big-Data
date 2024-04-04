package Q2;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class FriendshipDriver {
	public static void main(String[] args) throws Exception {
		 
    	if (args.length != 2) {
            System.err.println("Usage: FriendsDriver <input path> <output path>");
            System.exit(-1);
        }
        
    	
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FriendshipDriver");
        
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setJarByClass(FriendshipDriver.class);
        job.setMapperClass(FriendshipMapper.class);
        job.setReducerClass(FriendshipReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
       
        System.exit(job.waitForCompletion(true) ? 0 : 1);
}}
