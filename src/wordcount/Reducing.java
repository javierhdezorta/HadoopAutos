package wordcount;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducing extends Reducer<Text,FloatWritable,Text,FloatWritable>{

    public void reduce(Text key,Iterable<FloatWritable>  values,Context context)throws IOException,InterruptedException{

        Float sum=0F,avg=0f;
        int c=0;
        try{
            for(FloatWritable value: values){
                System.out.println(key+"\t"+value);
                sum+=value.get();
                c++;
            }
            //System.out.println(key);
            avg=sum/c;

            context.write(new Text(key.toString()),new FloatWritable(avg));
        }
        catch (InterruptedException | IOException ie){
            ie.printStackTrace();
        }

    }
}
