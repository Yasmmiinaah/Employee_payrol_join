import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmployeeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        if (line.length() == 0) {
            return;
           
        }     
        String[] parts = line.split(",");

        if (parts.length < 4) {
            return;
        }

        String employeeId = parts[0].trim();

        // Skip header
        if (employeeId.equalsIgnoreCase("employeeId")) {
            return;
        }

        String firstName = parts[1].trim();
        String lastName = parts[2].trim();
        String department = parts[3].trim(); 
        
        outKey.set(employeeId);
        outValue.set("emp~" + firstName + "," + lastName + "," + department);

        context.write(outKey, outValue);
    }
}
        