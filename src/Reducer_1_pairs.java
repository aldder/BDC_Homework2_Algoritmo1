import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Reducer_1_pairs extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable count = new IntWritable(0);

	@Override
	protected void reduce(Text word, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// System.out.println("----------------------------------REDUCER1---------------------------------------");
		for (IntWritable value : values) {
			count.set(count.get() + value.get());
		}

		context.write(word, count);
		count.set(0);
	}
}
