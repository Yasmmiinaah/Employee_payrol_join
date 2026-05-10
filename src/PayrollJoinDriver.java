import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PayrollJoinDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: PayrollJoinDriver <employees_input> <payroll_input> <output>");
            System.exit(1);
        }
        
        Configuration conf = new Configuration();

        conf.set("emp.tag", "emp~");
        conf.set("pay.tag", "pay~");

        Job job = Job.getInstance(conf, "Task 7 Employee Payroll Join");

        job.setJarByClass(PayrollJoinDriver.class);

        MultipleInputs.addInputPath(
        		 job,
                 new Path(args[0]),
                 TextInputFormat.class,
                 EmployeeMapper.class
         );

         MultipleInputs.addInputPath(
                 job,
                 new Path(args[1]),
                 TextInputFormat.class,
                 PayrollMapper.class
         );
         
         job.setReducerClass(PayrollReducer.class);

         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(Text.class);

         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(Text.class);

         job.setNumReduceTasks(1);

         Path outputPath = new Path(args[2]);
         FileSystem fs = FileSystem.get(conf);
         
         if (fs.exists(outputPath)) {
             fs.delete(outputPath, true);
         }

         FileOutputFormat.setOutputPath(job, outputPath);

         System.exit(job.waitForCompletion(true) ? 0 : 1);
     }
 }