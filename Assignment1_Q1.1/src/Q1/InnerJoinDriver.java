package Q1;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InnerJoinDriver {
    public static void main(String[] args) throws Exception {
    	InnerJoinMapper mapper;
    	if (args.length != 2) {
            System.err.println("Usage: InnerJoinDriver <input path> <output path>");
            System.err.println(args[0]+args[1]);

            System.exit(-1);
        }
        
        // Create a new job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InnerJoinDriver");
        job.setJarByClass(InnerJoinDriver.class);

        
        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // Set mapper, reducer, and output classes
        job.setMapperClass(InnerJoinMapper.class);
        job.setReducerClass(InnerJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
