import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Reducer_2 extends Reducer<Text, Text, Text, Text> {

	private final static String flag = new String("*");
	
	private Text first = new Text();
	private Text second = new Text();
	
	private List<String> StringList = new ArrayList<String>();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// System.out.println("----------------------------------REDUCER2----------------------------------------");

		int countB = 0;
		
		for (Text value : values) {
			String[] tokens = value.toString().split("\\s+");
			/*
			 * Scorre la lista di valori del tipo
			 * <B,(A,count(AB)><B,(C,count(CB)>...<B,(*,count(B))> Quando trova
			 * che il primo item della coppia è un '*', ha trovato il valore di
			 * count(B). Quindi salva i rimanenti item (contenenti le coppie
			 * (A,count(AB) in una lista ausiliaria.
			 */
			if (tokens[0].equals(flag)) {
				countB = Integer.parseInt(tokens[1].trim());
			} else {
				StringList.add(tokens[0]+" "+tokens[1]);
			}
		}
		for (String item : StringList) {
			String[] items = item.toString().split("\\s+");
			/*
			 * Scorre la lista dei valori dove la chiave relativa ad essi è B,
			 * e i valori sono (A,count(AB). Per ogni valore prende l'item
			 * count(AB), e calcola la probabilità condizionata
			 * count(AB)/count(B) per ogni coppia (AB) ed emette in output
			 * (AB), count(AB), Pr(A|B)
			 */
			float countAB = (float) Integer.parseInt(items[1].trim());
			
			first.set(items[0]+" "+key.toString());
			second.set(items[1]+" "+countAB / countB);
			
			context.write(first, second);
		}
		StringList.clear();
	}
}