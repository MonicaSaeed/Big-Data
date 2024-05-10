package Q2;

import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class FriendshipMapper  extends Mapper<Object, Text, Text, Text> {
	private Text person = new Text();
    private Text friend = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line into two persons
        String[] tokens = value.toString().split(",");

        // Emit key-value pairs for both persons
        person.set(tokens[0].trim());
        friend.set(tokens[1].trim());
        context.write(person, friend);

        person.set(tokens[1].trim());
        friend.set(tokens[0].trim());
        context.write(person, friend);
    }
}
