
package contapalavras;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ContaPalavrasMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable numeroUm = new IntWritable(1);
    private final Text palavra = new Text();

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        palavra.set(itr.nextToken());
        context.write(palavra, numeroUm);
      }
    }
}
