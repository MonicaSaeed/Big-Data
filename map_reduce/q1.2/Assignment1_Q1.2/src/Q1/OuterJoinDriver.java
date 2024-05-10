package Q1;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OuterJoinDriver {
    public static void main(String[] args) throws Exception {
    	if (args.length != 2) {
            System.err.println("Usage: OuterJoinDriver <input path> <output path>");
            System.err.println(args[0]+args[1]);

            System.exit(-1);
        }
        
        // Create a new job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "OuterJoinDriver");
        job.setJarByClass(OuterJoinDriver.class);

        
        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // Set mapper, reducer, and output classes
        job.setMapperClass(OuterJoinMapper.class);
        job.setReducerClass(OuterJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
