package com.soe.lishuai;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.*;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.aggregations.pipeline.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@SpringBootTest
public class ESForRule1_5 {

    private final RestHighLevelClient restHighLevelClient;


    @Autowired
    public ESForRule1_5(RestHighLevelClient restHighLevelClient){
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * 返回任意两个日期之间相差天数
     * @param one
     * @param two
     * @return
     */
    private static long daysBetween(Date one, Date two) {
        long difference =  (one.getTime()-two.getTime())/86400000;
        return Math.abs(difference);
    }

    /**
     * 测试函数
     * @throws ParseException
     */
    @Test
    public void testDaysBetween() throws ParseException {
        String sBeginDate = "20180301";
        String sEndDate = "20180405";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        System.out.println(daysBetween(sdf.parse(sBeginDate),sdf.parse(sEndDate)));
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(sBeginDate));
        calendar.add(calendar.DATE, 1);
        System.out.println(sdf.format(calendar.getTime()));
    }


    /**
     * 传入index和字段field，查询该index内 该field的最小值和最大值
     * 如：用于获取日期的最小值和最大值，以便按时间窗口轮询
     * @param index
     * @param field
     * @return
     */
    public String[] get_Min_Max(String index, String field){
        SearchRequest searchRequest = new SearchRequest(index);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        //最小值  最大值
        MinAggregationBuilder aggregation_min = AggregationBuilders
                .min("min")  //聚合名称
                .field(field);   //分组属性



        MaxAggregationBuilder aggregation_max = AggregationBuilders
                .max("max")  //聚合名称
                .field(field);   //分组属性

        searchSourceBuilder.aggregation(aggregation_max);
        searchSourceBuilder.aggregation(aggregation_min);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Aggregations aggregations = searchResponse.getAggregations();
        Min min = aggregations.get("min");
        Max max = aggregations.get("max");
        System.out.println("min = "+min.getValueAsString());
        System.out.println("max = "+max.getValueAsString());
        return new String[]{min.getValueAsString(),max.getValueAsString()};
    }

    @Test
    public void testGet_Min_Max(){
        System.out.println(get_Min_Max("tb_acc_txn","hour")[0]);
    }





    /**
     * 假设state为客户，按照3年年龄为时间窗口，统计每桶内性别为男M且balance总数大于某值的，并且统计count(city)
     *
     * "计算周期：每日（交易日期）
     * 通过表tb_acc_txn中
     * 字段：
     * Lend_flag=11：付
     * Tsf_flag=10：现金
     * Self_bank_code≥3个
     * 日累计取现交易金额≥500000
     * 进行条件过滤"
     */
    @Test
    public void rule_1_test() throws IOException, ParseException {

        String[] min_max = get_Min_Max("tb_acc_txn", "hour");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));
        for (int i=0;i<daysBetween;i++) {
            SearchRequest searchRequest = new SearchRequest("tb_acc_txn");

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        QueryBuilder query = QueryBuilders.matchQuery("Lend_flag","11");
            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            calendar.add(calendar.DATE, i);
// 这个时间就是日期往后推一天的结果
            String curDay = sdf.format(calendar.getTime());
//        一天为窗口
            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("hour").format("yyyyMMdd").gte(curDay).lte(curDay);
            ((BoolQueryBuilder) query).filter(queryBuilder1);
//        //1,查询Lend_flag=11：付
            QueryBuilder queryBuilder2 = QueryBuilders.termQuery("lend_flag", "11");
            ((BoolQueryBuilder) query).filter(queryBuilder2);
//
//        //2,查询Tsf_flag=10：现金
            QueryBuilder queryBuilder3 = QueryBuilders.termQuery("tsf_flag", "10");
            ((BoolQueryBuilder) query).filter(queryBuilder3);
//


            //嵌套子聚合查询  以self_acc_name账户名称分桶
            TermsAggregationBuilder agg_self_acc_name = AggregationBuilders.terms("agg_self_acc_no").field("cst_no")
                    .subAggregation(AggregationBuilders.cardinality("count_self_bank_code").field("self_bank_code"))
                    .subAggregation(AggregationBuilders.sum("sum_rmb_amt").field("rmb_amt"));

            //子聚合  管道聚合不能包含子聚合，但是某些类型的管道聚合可以链式使用（比如计算导数的导数）。
            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("count_self_bank_code", "count_self_bank_code");
        bucketsPath.put("sum_rmb_amt","sum_rmb_amt");
        Script script = new Script("params.count_self_bank_code >= 3 && params.sum_rmb_amt >= 50000");
//            Script script = new Script("params.count_self_bank_code>=3");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_self_acc_name.subAggregation(bs);

            agg_self_acc_name.subAggregation(AggregationBuilders.topHits("topHits").size(1000));
            searchSourceBuilder.aggregation(agg_self_acc_name);
            searchSourceBuilder.size(0);


            searchSourceBuilder.query(query);

            searchRequest.source(searchSourceBuilder);
//            System.out.println("查询条件：" + searchSourceBuilder.toString());

            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//            System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);  //这里并不是topHits的数量

            Aggregations aggregations = searchResponse.getAggregations();

            ParsedTerms txn_per_day = aggregations.get("agg_self_acc_no");
            // 获取到分组后的所有bucket
            List<? extends Terms.Bucket> buckets = txn_per_day.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                // 解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();

                ParsedTopHits topHits = bucketAggregations.get("topHits");

                int len = topHits.getHits().getHits().length;
                System.out.println(len);
                for (int j = 0; j < len; j++) {
                    Map<String, Object> sourceAsMap = topHits.getHits().getHits()[j].getSourceAsMap();
                    // 打印出统计后的数据
                    System.out.println(sourceAsMap);

                }


            }
//            restHighLevelClient.close();

        }

//        }

    }

    /**
     * "计算周期：三日（开户日期）
     * 通过表tb_acc中
     * 字段：
     * Open_type1=11：代理
     * Agent_no和Agent_name均不能为空
     * 三日累计开户数量≥10
     * 进行条件过滤"
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void rule_2_test() throws IOException, ParseException {
        File file = new File("result.txt");

        if (!file.exists()) {
            file.createNewFile();
        }
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);



        String[] min_max = get_Min_Max("tb_acc", "open_time");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));

        Calendar calendar2 = new GregorianCalendar();


        for (int i=0;i<daysBetween;i++) {
            SearchRequest searchRequest = new SearchRequest("tb_acc");

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            calendar.add(calendar.DATE, 1);
//            当前时间
            String curDay = sdf.format(calendar.getTime());
//            窗口起始时间
            String bDate = sdf.format(calendar.getTime());
            calendar2.setTime(sdf.parse(curDay));
//            窗口截至时间
            calendar2.add(calendar2.DATE, 2);
            String eDate = sdf.format(calendar2.getTime());
//            窗口复原
//            calendar2.setTime(sdf.parse(bDate));
//            System.out.println(bDate+"  -  "+eDate);
//        3天为窗口
            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("open_time").format("yyyyMMdd").gte(bDate).lte(eDate);
            ((BoolQueryBuilder) query).filter(queryBuilder1);
//        //1,open_type1=11：付
            QueryBuilder queryBuilder2 = QueryBuilders.termQuery("open_type1", "11");
            ((BoolQueryBuilder) query).filter(queryBuilder2);
//
//        //agent_no不能为空
            QueryBuilder queryBuilder3 = QueryBuilders.wildcardQuery("agent_no","*");
            ((BoolQueryBuilder) query).filter(queryBuilder3);
//            //agent_name不能为空
            QueryBuilder queryBuilder4 = QueryBuilders.wildcardQuery("agent_name","*");
            ((BoolQueryBuilder) query).filter(queryBuilder4);
//


            //嵌套子聚合查询  以agent_no分桶
            TermsAggregationBuilder agg_agent_no = AggregationBuilders.terms("agg_agent_no").field("agent_no")
                    .subAggregation(AggregationBuilders.cardinality("count_self_acc_no").field("self_acc_no")); //账户数量


            //子聚合  管道聚合不能包含子聚合
            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("count_self_acc_no", "count_self_acc_no");
            Script script = new Script("params.count_self_acc_no >= 10");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_agent_no.subAggregation(bs);

            agg_agent_no.subAggregation(AggregationBuilders.topHits("topHits").size(1000));
            searchSourceBuilder.aggregation(agg_agent_no);
            searchSourceBuilder.size(0);


            searchSourceBuilder.query(query);

            searchRequest.source(searchSourceBuilder);
//            System.out.println("查询条件：" + searchSourceBuilder.toString());

            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//            System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);  //这里并不是topHits的数量

            Aggregations aggregations = searchResponse.getAggregations();

            ParsedTerms txn_per_day = aggregations.get("agg_agent_no");
            // 获取到分组后的所有bucket
            List<? extends Terms.Bucket> buckets = txn_per_day.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                // 解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();

                ParsedTopHits topHits = bucketAggregations.get("topHits");

                int len = topHits.getHits().getHits().length;
                System.out.println(len);
                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                String r_date = (String) sourceAsMap.get("open_time");
                String r_agent_no = (String) sourceAsMap.get("agent_no");
                String r_cst_no = (String) sourceAsMap.get("cst_no");
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
                ParsedCardinality count_self_acc_no =  bucketAggregations.get("count_self_acc_no");
                System.out.println("日期="+r_date+", "+"客户号="+r_cst_no+", "+"客户名称="+r_self_acc_name+", 代理人身份证="+r_agent_no+", 开户数量="+count_self_acc_no.getValueAsString());

            }
//            restHighLevelClient.close();

        }

//        }

    }


}
