package question_3;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NameAgeDriver {
    public static void main(String[] args) throws Exception {
        /*
    	if (args.length != 2) {
            System.err.println("Usage: NameAgeDriver <input path> <output path>");
            System.exit(-1);
        }
        */
     // Create a new job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NameAgeDriver");
        job.setJarByClass(NameAgeDriver.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
     // Set mapper, reducer, and output classes
        job.setMapperClass(NameAgeMapper.class);
        job.setReducerClass(NameAgeWriterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
     // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
