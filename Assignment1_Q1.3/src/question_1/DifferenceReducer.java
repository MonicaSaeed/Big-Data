package question_1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DifferenceReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
    	// check if the length of values list is > 1
    	int count = 0;
    	for(Text value : values){
    		count++;
    	}
    	
        if(count == 1) {
            context.write(key, null);
        }
    }
}