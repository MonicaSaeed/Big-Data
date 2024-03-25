package question_3;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NameAgeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text name = new Text();
    private DoubleWritable age = new DoubleWritable();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the CSV row by comma
        String[] row = value.toString().split(",");

        // Check if the row has exactly two columns (name and age)
        if (row.length == 2) {
            // Extract name
            String nameValue = row[0].trim();
         // Extract and parse age to double
            double ageValue = 0;
            try {
                ageValue = Double.parseDouble(row[1].trim());
            } catch (NumberFormatException e) {
                // Handle parse error or ignore the record
            	 return; // Skip this record
            }

            // Emit name and age as key-value pairs
            name.set(nameValue);
            age.set(ageValue);
            context.write(name, age);
        }
    }
}