
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class Join {

    public static class AgeFileMapper extends
					  Mapper<Text, Text, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();

	public void map(Text key, Text value, Context context) 
	    throws IOException, InterruptedException {
	    
	    // fill in here
		StringBuilder sb=new StringBuilder();
		sb.append(value.toString());
		sb.append("a");
		outputKey.set(key.toString());
		outputValue.set(sb.toString());
		context.write(outputKey, outputValue);
		
	
	}    
    }

    public static class WeightFileMapper extends
					  Mapper<Text, Text, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();

	public void map(Text key, Text value, Context context) 
	    throws IOException, InterruptedException {
	    
	    // fill in here
		
		StringBuilder sb=new StringBuilder();
		sb.append(value.toString());
		sb.append("w");
		outputKey.set(key.toString());
		outputValue.set(sb.toString());
		context.write(outputKey, outputValue);
	}    
    }

    public static class JoinReducer extends
					Reducer<Text, Text, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException {

	    // fill in here
		StringBuilder sb=new StringBuilder();
		String age="";
		String weight="";
        for (Text t:values){
            String fn=t.toString();
            if (fn.endsWith("a")){
            	age=fn.substring(0,fn.length()-1);
            }
            else{
            	weight=fn.substring(0,fn.length()-1);
            }
        }
        if (!age.equals("") && !weight.equals("")){
        	sb.append("(");
        	sb.append(age);
        	sb.append(", ");
        	sb.append(weight);
        	sb.append(")");
        }
        String op=sb.toString();
        if (op.contains(")"))
        {
        	outputKey.set(key.toString());
        	outputValue.set(op);
        	context.write(outputKey, outputValue);
        }
		
	}
    }

    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	
	if (args.length != 3) {
	    System.err.println("Usage: Join <age-in> <weight-in> <out>");
	    System.exit(2);
	}
	
	
      Job job = Job.getInstance(conf, "join");
	  //IF ERROR IN THIS LINE DUE TO getInstance please replace this line with following line:-
	  //Job job = new Job(conf, "join");
      job.setJarByClass(Join.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setReducerClass(JoinReducer.class);

      MultipleInputs.addInputPath(job, 
				  new Path(args[0]), 
				  KeyValueTextInputFormat.class, 
				  AgeFileMapper.class);

      MultipleInputs.addInputPath(job, 
				  new Path(args[1]), 
				  KeyValueTextInputFormat.class, 
				  WeightFileMapper.class);

      FileOutputFormat.setOutputPath(job, new Path(args[2]));

      System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
