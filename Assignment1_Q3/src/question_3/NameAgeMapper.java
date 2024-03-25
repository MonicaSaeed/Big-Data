package question_3;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NameAgeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text name = new Text();
    private DoubleWritable age = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] row = value.toString().split(",");

        // Check if the row has exactly two columns (name and age)
        if (row.length == 2) {
            String nameValue = row[0].trim();
            double ageValue = 0;
            try {
                ageValue = Double.parseDouble(row[1].trim());
            } catch (NumberFormatException e) {
                return;
            }

            name.set(nameValue);
            age.set(ageValue);
            context.write(name, age);
        }
    }
}