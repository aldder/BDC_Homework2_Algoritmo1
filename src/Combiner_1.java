import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Set;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Combiner_1 extends Reducer<Text, MapWritable, Text, MapWritable> {

	private MapWritable finalStripe = new MapWritable();

	@Override
	protected void reduce(Text word, Iterable<MapWritable> stripes, Context context)
			throws IOException, InterruptedException {
		// System.out.println("----------------------------------REDUCER1---------------------------------------");

		for (MapWritable stripe : stripes) {
			mergeStripes(stripe);
		}
		
		context.write(word, finalStripe);
		
		finalStripe.clear();
	}

	private void mergeStripes(MapWritable mapWritable) {
		Set<Writable> keys = mapWritable.keySet();
		for (Writable key : keys) {
			IntWritable fromCount = (IntWritable) mapWritable.get(key);
			if (finalStripe.containsKey(key)) {
				IntWritable count = (IntWritable) finalStripe.get(key);
				count.set(count.get() + fromCount.get());
				finalStripe.replace(key, count);
			} else {
				finalStripe.put(key, fromCount);
			}
		}
	}
}
