import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		// TODO Auto-generated method stub
		if(args[0].equals("offline"))
		{
			Configuration conf = new Configuration();
			ArrayList<Path> inputs = new ArrayList<Path>();
			
			Job TFjob=Job.getInstance(conf);
			Path input = new Path(args[1]);
			inputs.add(input);
			Path output = new Path("TrainTFOutput");
			configureJob(TFjob, TermFrequencyMapper.class, TermFrequencyReducer.class, NgramAuthor.class, Text.class, Text.class, IntWritable.class, inputs, output);
			TFjob.waitForCompletion(true);
			
			Job AugTFJob=Job.getInstance(conf);
			inputs.clear();
			input = new Path("TrainTFOutput");
			inputs.add(input);
			output = new Path("TrainAugTFOutput");
			configureJob(AugTFJob, TrainAugmentedTFMapper.class, TrainAugmentedTFReducer.class, Text.class, WordCount.class, Text.class, DoubleWritable.class, inputs, output);
			AugTFJob.waitForCompletion(true);
			
			Job AuthorCount=Job.getInstance(conf);
			inputs.clear();
			input = new Path(args[1]);
			inputs.add(input);
			output = new Path("TrainAuthorCountOutput");
			configureJob(AuthorCount, AuthorCountMapper.class, AuthorCountReducer.class, Text.class, Text.class, Text.class, IntWritable.class, inputs, output);
			AuthorCount.waitForCompletion(true);
			
			Job DocumentTF=Job.getInstance(conf);
			inputs.clear();
			input = new Path("TrainTFOutput");
			inputs.add(input);
			output = new Path("TrainDTFOutput");
			configureJob(DocumentTF, DocumentTermFrequencyMapper.class, DocumentTermFrequencyReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, inputs, output);
			DocumentTF.waitForCompletion(true);
			
			Job IDF=Job.getInstance(conf);
			inputs.clear();
			input = new Path("TrainAuthorCountOutput");
			inputs.add(input);
			Path input1 = new Path("TrainDTFOutput");
			inputs.add(input1);
			output = new Path("TrainIDFOutput");
			configureJob(IDF, IDFMapper.class, IDFReducer.class, Text.class, Text.class, Text.class, DoubleWritable.class, inputs, output);
			IDF.waitForCompletion(true);
			
			Job AuthorIDFCombine=Job.getInstance(conf);
			inputs.clear();
			input = new Path("TrainAuthorCountOutput");
			inputs.add(input);
			input1 = new Path("TrainIDFOutput");
			inputs.add(input1);
			output = new Path("TrainAuthorIDFOutput");
			configureJob(AuthorIDFCombine, AuthorIDFMapper.class, AuthorIDFReducer.class, Text.class, Text.class, Text.class, Text.class, inputs, output);
			AuthorIDFCombine.waitForCompletion(true);
			
			
			Job AAV=Job.getInstance(conf);
			inputs.clear();
			input = new Path("TrainAuthorIDFOutput");
			inputs.add(input);
			input1 = new Path("TrainAugTFOutput");
			inputs.add(input1);
			output = new Path("TrainAAVOutput");
			configureJob(AAV, TrainAAVMapper.class, TrainAAVReducer.class, Text.class, Text.class, Text.class, DoubleWritable.class, inputs, output);
			System.exit(AAV.waitForCompletion(true) ? 0 : 1);
		}
		else if(args[0].equals("online"))
		{
			Configuration conf = new Configuration();
			ArrayList<Path> inputs = new ArrayList<Path>();
			
			Job TFjob=Job.getInstance(conf);
			Path input = new Path(args[1]);
			inputs.add(input);
			Path output = new Path("TFOutput");
			configureJob(TFjob, TermFrequencyMapper.class, TermFrequencyReducer.class, NgramAuthor.class, Text.class, Text.class, IntWritable.class, inputs, output);
			TFjob.waitForCompletion(true);
			
			Job AugTFJob=Job.getInstance(conf);
			inputs.clear();
			input = new Path("TFOutput");
			inputs.add(input);
			output = new Path("AugTFOutput");
			configureJob(AugTFJob, AugmentedTFMapper.class, AugmentedTFReducer.class, Text.class, WordCount.class, Text.class, DoubleWritable.class, inputs, output);
			AugTFJob.waitForCompletion(true);
			
			Job AAV=Job.getInstance(conf);
			inputs.clear();
			input = new Path("AugTFOutput");
			inputs.add(input);
			Path input1 = new Path("TrainIDFOutput");
			inputs.add(input1);
			output = new Path("AAVOutput");
			configureJob(AAV, AAVMapper.class, AAVReducer.class, Text.class, Text.class, Text.class, DoubleWritable.class, inputs, output);
			AAV.waitForCompletion(true);
			
			Job SimMult=Job.getInstance(conf);
			inputs.clear();
			input = new Path("AAVOutput");
			inputs.add(input);
			input1 = new Path("TrainAAVOutput");
			inputs.add(input1);
			output = new Path("SimMultOutput");
			configureJob(SimMult, SimMultMapper.class, SimMultReducer.class, Text.class, Text.class, Text.class, DoubleWritable.class, inputs, output);
			SimMult.waitForCompletion(true);
			
			Job SimSum=Job.getInstance(conf);
			inputs.clear();
			input = new Path("SimMultOutput");
			inputs.add(input);
			output = new Path("SimSumOutput");
			configureJob(SimSum, SimSumMapper.class, SimSumReducer.class, Text.class, DoubleWritable.class, Text.class, DoubleWritable.class, inputs, output);
			SimSum.setCombinerClass(SimSumReducer.class);
			SimSum.waitForCompletion(true);
			
			Job SimSquareSum=Job.getInstance(conf);
			inputs.clear();
			input = new Path("TrainAAVOutput");
			inputs.add(input);
			input1 = new Path("AAVOutput");
			inputs.add(input1);
			output = new Path("AAVSquareSumRootOutput");
			configureJob(SimSquareSum, SimSquareSumMapper.class, SimSquareSumReducer.class, Text.class, DoubleWritable.class, Text.class, DoubleWritable.class, inputs, output);
			SimSquareSum.waitForCompletion(true);
			
			Job SimMagMult=Job.getInstance(conf);
			inputs.clear();
			input = new Path("AAVSquareSumRootOutput");
			inputs.add(input);
			output = new Path("SimMagMultOutput");
			configureJob(SimMagMult, SimMagMultMapper.class, SimMagMultReducer.class, Text.class, Text.class, Text.class, DoubleWritable.class, inputs, output);
			SimMagMult.waitForCompletion(true);
			
			Job Divide=Job.getInstance(conf);
			inputs.clear();
			input = new Path("SimMagMultOutput");
			inputs.add(input);
			input1 = new Path("SimSumOutput");
			inputs.add(input1);
			output = new Path("SimilarityOutput");
			configureJob(Divide, DivideMapper.class, DivideReducer.class, Text.class, Text.class, Text.class, DoubleWritable.class, inputs, output);
			Divide.waitForCompletion(true);
			
			Job topTen=Job.getInstance(conf);
			inputs.clear();
			input = new Path("SimilarityOutput");
			inputs.add(input);
			output = new Path("TopTenSimilar");
			configureJob(topTen, topTenMapper.class, topTenReducer.class, Text.class, Text.class, Text.class, Text.class, inputs, output);
			System.exit(topTen.waitForCompletion(true) ? 0 : 1);
		}
		else 
		{
			 System.out.println("Usage: <jar file> train <training dir>\n");
			 System.out.println("Usage: <jar file> run <run sample dir>\n");
			 System.exit(-1);
		}
	}
	
	public static void configureJob(Job job, Class MapperClass, Class ReducerClass, Class MapOutputKeyClass, Class MapOutputValueClass, Class OutputKeyClass, Class OutputValueClass, ArrayList<Path> inputs, Path output) throws IOException, InterruptedException, ClassNotFoundException
	{
		job.setJarByClass(Main.class);
		job.setMapperClass(MapperClass);
		job.setReducerClass(ReducerClass);
		
		job.setMapOutputKeyClass(MapOutputKeyClass);
		job.setMapOutputValueClass(MapOutputValueClass);
		
		job.setOutputKeyClass(OutputKeyClass);
		job.setOutputValueClass(OutputValueClass);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		for(int i = 0; i < inputs.size(); i++)
		{
			FileInputFormat.addInputPath(job, inputs.get(i));
		}
		FileOutputFormat.setOutputPath(job, output);
		
	}

}
