
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 *
 * @author rpereira
 */
public class ContaHashtagsMapper 
    extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable numeroUm = new IntWritable(1);
    private final Text palavra = new Text();

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        
        StringTokenizer tk = new StringTokenizer(value.toString());
        while (tk.hasMoreTokens()) {
            String token = tk.nextToken();
            if(token.startsWith("#")){
                 palavra.set(token.toLowerCase()
                         .replaceAll("[^a-zA-Z# ]", ""));
                 context.write(palavra, numeroUm);
             }
        }
    }
}