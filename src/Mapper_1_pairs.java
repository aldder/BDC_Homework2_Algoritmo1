import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Mapper_1_pairs extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable ONE = new IntWritable(1);
	private final static Text flag = new Text("*");

	private Text pair = new Text();

	/*
	 * Lista contenente i valori visitati, se si incontra un valore visitato
	 * precedentemente si salta affinchè il conteggio non venga ripetuto più di
	 * una volta
	 */
	private List<String> sameValues = new ArrayList<String>();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// System.out.println("----------------------------------MAPPER1---------------------------------------");

		String[] tokens = value.toString().split("\\s+");
		int size = tokens.length;

		if (size > 0) {

			for (int i = 0; i < size; i++) {

				if (sameValues.contains(tokens[i]))
					continue;
				else
					sameValues.add(tokens[i]);

				pair.set(flag + " " + tokens[i]);
				context.write(pair, ONE);

				for (int j = i + 1; j < size; j++) {

					if (tokens[i].equals(tokens[j]))
						continue;

					pair.set(tokens[i] + " " + tokens[j]);
					context.write(pair, ONE);
					pair.set(tokens[j] + " " + tokens[i]);
					context.write(pair, ONE);
				}
			}
			sameValues.clear();
		}
	}
}
