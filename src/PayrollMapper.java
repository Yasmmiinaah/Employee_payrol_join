import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PayrollMapper extends Mapper<LongWritable, Text, Text, Text> {

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

        if (parts.length < 5) {
            return;
        }

        String payrollId = parts[0].trim();

        // Skip header
        if (payrollId.equalsIgnoreCase("payrollId")) {
            return;
        }

        String employeeId = parts[1].trim();
        String month = parts[2].trim();

        try {
        	 int baseSalary = Integer.parseInt(parts[3].trim());
             int bonus = Integer.parseInt(parts[4].trim());

             outKey.set(employeeId);
             outValue.set("pay~" + month + "," + baseSalary + "," + bonus);

             
             context.write(outKey, outValue);
             
             
        } catch (NumberFormatException e) {
            // Skip payroll lines with invalid salary or bonus
            context.getCounter("PayrollMapper", "INVALID_SALARY_OR_BONUS").increment(1);
        }
    }
}

        
