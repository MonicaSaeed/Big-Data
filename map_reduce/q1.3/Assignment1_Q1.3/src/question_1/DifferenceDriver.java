package question_1;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DifferenceDriver {
    public static void main(String[] args) throws Exception {
        
    	if (args.length != 2) {
            System.err.println("Usage: DifferenceDriver <input path> <output path>");
            System.exit(-1);
        }
        
        // Create a new job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DifferenceDriver");
        job.setJarByClass(DifferenceDriver.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // Set mapper, reducer, and output classes
        job.setMapperClass(DifferenceMapper.class);
        job.setReducerClass(DifferenceReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
