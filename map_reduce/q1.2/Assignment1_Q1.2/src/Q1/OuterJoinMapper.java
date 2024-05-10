package Q1;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class OuterJoinMapper extends Mapper<Object, Text, Text, Text> {


	private Text outKey = new Text();
    private Text outValue = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens.length == 3 && tokens[0].equals("T1")) {
            outKey.set(tokens[1].trim());
            outValue.set(tokens[0].trim() + "," + tokens[2].trim());
            context.write(outKey,outValue);
        }else if (tokens.length == 3 && tokens[0].equals("T2")) {
        	
            outKey.set(tokens[2].trim());
            outValue.set(tokens[0].trim() + "," + tokens[1].trim());
            context.write(outKey,outValue);
        }
    }
}
