import bean.Case;
import bean.CaseFactory;
import bean.CaseForResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
 * */

public class SimpleAlogrithm {

    public void run(String inputDataPath,String outputDataPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.tracker","hdfs://127.0.0.1:8001");

        Job job = Job.getInstance(conf, "coupon");
        job.setJarByClass(SimpleAlogrithm.class);
        job.setMapperClass(SimpleAlogrithm.LSHMapper.class);
        //job.setCombinerClass(SimpleAlogrithm.IntSumReducer.class);
        job.setReducerClass(SimpleAlogrithm.SimilarReducer.class);
        job.setOutputKeyClass(Case.class);
        job.setOutputValueClass(FloatWritable.class);

        CSVInputFormat.addInputPath(job, new Path(inputDataPath));
        CSVOutputFormat.setOutputPath(job, new Path(outputDataPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * 输入：行号，记录
     * 输出：hashcode，case
     */
    static class LSHMapper extends Mapper<LongWritable, TextArrayWritable, LongWritable, Case> {

        private static LSHStrategy lsh = new LSHStrategy();

        @Override
        protected void map(LongWritable key, TextArrayWritable value, Context context) throws IOException, InterruptedException {

            Text[] texts = (Text[]) value.get();
            Case aCase = CaseFactory.getCase(texts);
            lsh.setRecord(CaseFactory.getFeatures(aCase));
            context.write(lsh.hash(),aCase);
        }
    }

    /**
     * 输入：hashcode，case
     * 输出：CaseForResult，核销概率
     */
    static class SimilarReducer extends Reducer<LongWritable,Case, CaseForResult,FloatWritable> {

        private static DateFormat dateFormat = new SimpleDateFormat("yyMMdd");
        private static Date date_20160701 ;

        static {
            try {
                date_20160701 = dateFormat.parse("20160701");
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void reduce(LongWritable key, Iterable<Case> values, Context context) throws IOException, InterruptedException {

            //具有同个key的case有可能相似
            //计算和当前value最相似的记录，然后输出
            for (Case aCase:
                 values) {
                Date date = null ;
                try {
                    date = dateFormat.parse((aCase.date)) ;
                } catch (ParseException e) {
                    e.printStackTrace();
                    continue;
                }
                if (date.after(date_20160701)){
                    //2016年7月1日之后的数据，需要我们猜测核销概率
                    HashMap<Case,Integer> similarCase = new SimilarFinder(aCase,values).find(5);
                    float couponVerfiedProbility = new ProbilityCalculator(similarCase).calculate();
                    context.write(new CaseForResult(aCase.userID,aCase.couponID,aCase.dateReceive),new FloatWritable(couponVerfiedProbility));
                }
            }
        }
    }
}
