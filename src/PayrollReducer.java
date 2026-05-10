import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PayrollReducer extends Reducer<Text, Text, Text, Text> {

    private String empTag;
    private String payTag;
    
    
    static class PayrollRecord {
        String month;
        int totalPay;

        PayrollRecord(String month, int totalPay) {
            this.month = month;
            this.totalPay = totalPay;
        }
    }
    
    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        empTag = conf.get("emp.tag", "emp~");
        payTag = conf.get("pay.tag", "pay~");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
    	 String fullName = "UNKNOWN EMPLOYEE";
         String department = "UNKNOWN";

         ArrayList<PayrollRecord> payrollRecords = new ArrayList<PayrollRecord>();
         int maxPay = Integer.MIN_VALUE;

         for (Text value : values) {
             String record = value.toString().trim();
             
             if (record.startsWith(empTag)) {
                 String empData = record.substring(empTag.length());
                 String[] parts = empData.split(",");

                 if (parts.length >= 3) {
                     String firstName = parts[0].trim();
                     String lastName = parts[1].trim();
                     department = parts[2].trim();

                     fullName = firstName + " " + lastName;
                 }
                 
             } else if (record.startsWith(payTag)) {
                 String payData = record.substring(payTag.length());
                 String[] parts = payData.split(",");

                 if (parts.length >= 3) {
                     try {
                         String month = parts[0].trim();
                         int baseSalary = Integer.parseInt(parts[1].trim());
                         int bonus = Integer.parseInt(parts[2].trim());
                         
                         int totalPay = baseSalary + bonus;

                         payrollRecords.add(new PayrollRecord(month, totalPay));

                         if (totalPay > maxPay) {
                             maxPay = totalPay;
                         }

                     } catch (NumberFormatException e) {
                         context.getCounter("PayrollReducer", "INVALID_PAY_RECORD").increment(1);
                     }
                 }
             }
         }
         
         for (PayrollRecord p : payrollRecords) {
             String output = fullName + "," +
                     department + "," +
                     p.month + "," +
                     p.totalPay + "," +
                     maxPay;

             context.write(key, new Text(output));
         }
     }
 }

    