package Q1;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class OuterJoinReducer extends Reducer<Text, Text, Text, Text> {
	private Text outValue1 = new Text();
	private Text outValue2 = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
       
		List<String> t1Values = new ArrayList<>();
        List<String> t2Values = new ArrayList<>();
        for (Text value : values) {
            String[] tokens = value.toString().split(",");
            if (tokens[0].equals("T1")) {
                t1Values.add(tokens[1]);
            } else if (tokens[0].equals("T2")) {
                t2Values.add(tokens[1]);
            }
        }
        
        if (!t1Values.isEmpty() && !t2Values.isEmpty()) {
        	for (String t1Value : t1Values) {
                for (String t2Value : t2Values) {
                    outValue1.set(t2Value +",");
                    outValue2.set(key+","+t1Value );
                    context.write(outValue1, outValue2);            }
            }
        }else if(t2Values.isEmpty() && !t1Values.isEmpty())
        {
        	 for (String t1Value : t1Values) {
        		 outValue1.set(null+", "+t1Value);
                 context.write(key,outValue1);
             }
        }
        else if(!t2Values.isEmpty() && t1Values.isEmpty())
        {
        	 for (String t2Value : t2Values) {
        		 outValue1.set(t2Value+", "+null);
                 context.write(outValue1,key);
             }
        }
        
        }
    
    }

