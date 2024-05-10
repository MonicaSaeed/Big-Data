package question_1;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;

public class DifferenceMapper extends Mapper<Object, Text, IntWritable, Text> {
	private IntWritable pk = new IntWritable();
	private Text tableName = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split 
        String[] row = value.toString().split(",");

        // Check if the row has exactly 3 columns
        if (row.length == 3) {
        	int pkValue = 0;
        	String tableNameValue;
        	if(row[0].trim().equals("T1")){
                 try {
                     pkValue = Integer.parseInt(row[1].trim());
                 } catch (NumberFormatException e) {
                 	 return; // Skip this record
                 }
        		tableNameValue = row[0].trim();
        	}else{
                try {
                    pkValue = Integer.parseInt(row[2].trim());
                } catch (NumberFormatException e) {
                	 return; // Skip this record
                }
                tableNameValue = row[0].trim();
        	}
	    

        // Emit name and age as key-value pairs
        pk.set(pkValue);
        tableName.set(tableNameValue);
        context.write(pk, tableName);
        }
    }
}