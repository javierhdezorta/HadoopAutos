package wordcount;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapping extends Mapper<Object,Text,Text,FloatWritable>{

    private Text CYL = new Text();
    private final static FloatWritable MPG = new FloatWritable(1);

    public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
        try{
            String[] columns=value.toString().split(",");
            //System.out.println(Arrays.toString(columns));
            String mpg= columns[0].trim();
            String cylinders= columns[1].trim();

            CYL.set(cylinders);
            MPG.set(Float.parseFloat(mpg));
            System.out.println(CYL+"----"+MPG);

            context.write(CYL, MPG);//word es lo que contiente key y one es el equivalente a 1 y va contando de 1 en 1.
        }
        catch (InterruptedException | IOException ie){
            ie.printStackTrace();
        }
    }
}
