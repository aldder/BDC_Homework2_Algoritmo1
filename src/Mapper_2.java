import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Mapper_2 extends Mapper<LongWritable, Text, Text, Text> {

	private Text first = new Text();
	private Text second = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// System.out.println("----------------------------------MAPPER2----------------------------------------");

		String[] tokens = value.toString().split("\\s+");
		/*
		 * Esegue lo swap tra gli item: <(A,B),count(AB)> -> <B,(A,count(AB))>
		 * affinchè al reducer arrivino tutte le informazioni relative alla
		 * chiave B, ossia il numero di occorrenze di B e la lista dei vicini ed
		 * il relativo conteggio
		 */
		if (tokens.length == 3) {
			first.set(tokens[1]);
			second.set(tokens[0]+" "+tokens[2]);
			context.write(first, second);
		}
	}
}
