import bean.Case;
import bean.CaseFactory;
import bean.CaseForResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.CSVInputFormat;
import util.CSVOutputFormat;
import util.TextArrayWritable;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by yjy on 17-7-2.
 */

/**
 * 题目描述：
 * 本题选自天池竞赛新人实战赛第一题（贼难啊...）
 * <a href="https://tianchi.aliyun.com/getStart/introduction.htm?spm=5176.100066.333.1.5908a109DqAeuV&raceId=231593">戳我查看题目</>
 *
 * 个人思路：
 * Mapper：从训练集导入记录，提取特征值，然后局部敏感hash；
 * Reducer：具有相同签名的记录有相似的可能性，
 * 从该记录相似的集合中找出与“待推测记录”最相似的5条，以这5条记录是否被核销来推测“待推测记录”的核销概率；
 *
 * 关键问题：
 * 特征值的选取；
 * 局部敏感函数的选取；
 * 核销概率的计算；
 *
 * 缺点：
 * 不懂得如何划分训练集、测试集，更不懂得如何使用测试集，所以所有数据统统当做训练集输入，因此算法有效性没有无法保证；
 * 特征值选取非常粗糙，因此模型拟合很差；
 *
 * 感受：
 * 业务需求本来还有分线上线下的优惠券的，太复杂了，把所有线下的都剥离，简化问题；
 * 数据之中的字段并不是单独的信息，字段之间的组合、计算其实也是很重要的特征，
 * 例如，同个用户在同家商户有多条记录的话，那么该用户在该家商户的核销率直观来看应该是会更高些的，
 * 一张优惠券第一天买第二天就用了，那么下次这个用户对于同一家商户同样折扣率的优惠券是不是有更高可能性会核销，
 * 诸如此类的特征组合非常多，本题排名第一的好像搞了30多个特征，不过这样提取特征值问题太难了我搞不定；
 *
 *
 * 网上查找这道题的做法，说是二分类问题。然而本题要求输出的是概率而不是“会不会”，所以我不知道怎么用二分类来做。自己选择算法。
 * 大家都是用的Python作答，用XGBoost建模，然而我完全不懂，就照着书来了。
 * 特征值的分析提取是最关键的一环。没有什么大数据的经验不太懂如何提取特征值。网上的思路好像提取了30多个特征，有些还是看都看不懂的，有些是能理解但是不知道如何实现的。最后根据自己的理解提取了11个特征。
 * */

public class SimpleAlogrithm {

    public static void run(String inputDataPath,String outputDataPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.tracker","hdfs://127.0.0.1:8001");

        Job job = Job.getInstance(conf, "coupon");
        job.setJarByClass(Main.class);
        job.setMapperClass(SimpleAlogrithm.LSHMapper.class);
        job.setCombinerClass(SimpleAlogrithm.SimilarReducer.class);
        job.setReducerClass(SimpleAlogrithm.SimilarReducer.class);

        job.setInputFormatClass(CSVInputFormat.class);
        job.setOutputFormatClass(CSVOutputFormat.class);
/*
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextArrayWritable.class);
*/
        CSVInputFormat.addInputPath(job, new Path(inputDataPath));
        CSVOutputFormat.setOutputPath(job, new Path(outputDataPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * 输入：行号，记录
     * 输出：hashcode，记录
     */
    static class LSHMapper extends Mapper<LongWritable, TextArrayWritable, Text, TextArrayWritable> {

        private static LSHStrategy lsh = new LSHStrategy();

        @Override
        protected void map(LongWritable key, TextArrayWritable value, Context context) throws IOException, InterruptedException {

            Text[] texts = (Text[]) value.get();
            Case aCase = CaseFactory.getCase(texts);
            lsh.setRecord(CaseFactory.getFeatures(aCase));
            String hashCode = lsh.hash();
            context.write(new Text(hashCode),value);
        }
    }

    /**
     * 输入：hashcode，record
     * 输出：""，record+核销概率
     */
    static class SimilarReducer extends Reducer<Text,TextArrayWritable, Text,TextArrayWritable> {

        private static DateFormat dateFormat = new SimpleDateFormat("yyMMdd");
        private static Date date_20160615 ;
        private static final Text emptyText = new Text("");

        static {
            try {
                date_20160615 = dateFormat.parse("20160615");
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {

            //具有同个key的case有可能相似
            //计算和当前value最相似的记录，然后输出
            for (TextArrayWritable arrayWritable:
                 values) {
                Case aCase = CaseFactory.getCase((Text[]) arrayWritable.get());
                Date date = null ;
                try {
                    date = dateFormat.parse((aCase.dateReceive)) ;
                } catch (ParseException e) {
                    e.printStackTrace();
                    continue;
                }
                if (date.after(date_20160615)){
                    //2016年7月1日之后的数据，需要我们猜测核销概率
                    HashMap<Case,Integer> similarCase = new SimilarFinder(aCase,values).find(5);
                    float couponVerfiedProbility = new ProbilityCalculator(similarCase).calculate();
                    Writable[] record = arrayWritable.get();
                    Writable[] writables={record[0],record[1],record[5],new FloatWritable(couponVerfiedProbility)};
                    arrayWritable.set(writables);
                    context.write(emptyText,arrayWritable);
                }
            }
        }
    }
}
