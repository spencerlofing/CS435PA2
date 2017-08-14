import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DivideReducer extends Reducer<Text, Text, Text, DoubleWritable>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		double numerator = 1;
		double denom = 1;
		for(Text val : values)
		{
			String[] parse = val.toString().split("\t");
			String nORd = parse[0];
			double number = Double.parseDouble(parse[1]);
			if(nORd.equals("numerator"))
			{
				numerator = number;
			}
			else
			{
				denom = number;
			}
		}
		double divide = numerator/denom;
		context.write(key, new DoubleWritable(divide));
	}

}
