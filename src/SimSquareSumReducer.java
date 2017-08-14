import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SimSquareSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
	{
		double sum = 0;
		for(DoubleWritable val : values)
		{
			sum+=val.get();
		}
		double sqrtSum = Math.sqrt(sum);
		context.write(key, new DoubleWritable(sqrtSum));
		
	}

}
