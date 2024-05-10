package question_2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendsMapper extends Mapper<Object, Text, Text, Text> {
    private Text ky = new Text();
    private Text val = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        for (int i = 1; i < 6; i++) {
            ky.set("P"+Integer.toString(i));
            val.set(value.toString());
            context.write(ky, val);
        }
    }
}
