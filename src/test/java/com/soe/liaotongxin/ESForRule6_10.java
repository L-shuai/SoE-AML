package com.soe.liaotongxin;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.aggregations.pipeline.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.soe.utils.ESUtils.daysBetween;

@SpringBootTest
public class ESForRule6_10 {
    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient restHighLevelClient;

    public String[] get_Min_Max(String index, String field,QueryBuilder queryBuilder){
        SearchRequest searchRequest = new SearchRequest(index);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        if(queryBuilder!=null){
            searchSourceBuilder.query(queryBuilder);
        }else{
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        }


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
    /**
     * "计算周期：每日
     * 通过表tb_acc_txn中
     * 字段：
     * Part_acc_no：交易对手卡号、账号
     * Lend_flag=10：资金收付为收
     * 日累计交易不同交易对手个数≥3
     **/
    @Test
    public void rule_6_test() throws IOException, ParseException {
    //获取最大和最小日期范围
        String[] min_max = get_Min_Max("tb_acc_txn", "date2",null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));
        SearchRequest searchRequest = new SearchRequest("tb_acc_txn");
        for (int i = 0; i < daysBetween; i++) {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            calendar.add(calendar.DATE, 1);
            String curDay = sdf.format(calendar.getTime());

            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
            //过滤日期
            queryBuilder.filter(QueryBuilders.termQuery("date2", curDay));
            //过滤资金收付为收
            queryBuilder.filter(QueryBuilders.termQuery("lend_flag", "10"));
            //按账户名进行分桶
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no")
                    .subAggregation(AggregationBuilders.cardinality("count_part_acc_no").field("part_acc_no"));

            //按交易账户数量大于等于3过滤
            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("count_part_acc_no", "count_part_acc_no");
            Script script = new Script("params.count_part_acc_no >= 3");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_self_acc_no.subAggregation(bs);
            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(1000));
            searchSourceBuilder.query(queryBuilder);
            searchSourceBuilder.aggregation(agg_self_acc_no);
            searchSourceBuilder.size(0);

            searchRequest.source(searchSourceBuilder);
//            System.out.println("查询条件：" + searchSourceBuilder.toString());
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//            System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);

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
        }

    }
    /**
     * "计算周期：三日（交易日期）
     * 通过表tb_cred_txn中
     * 字段：
     * Org_amt:原币种交易金额
     * 计算三日总交易笔数
     * 计算三日原币种交易金额≥1000且是100的整数倍的整百交易笔数
     * 三日整数倍的整百交易笔数≥三日累计总交易笔数*60%
     **/
    @Test
    public void rule_7_test() throws IOException, ParseException{
        String[] min_max = get_Min_Max("tb_cred_txn", "date",null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));

        Calendar calendar2 = new GregorianCalendar();

        SearchRequest searchRequest = new SearchRequest("tb_cred_txn");

        for (int i=0;i<daysBetween;i++) {

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            calendar.add(calendar.DATE, 1);
            //当前时间
            String curDay = sdf.format(calendar.getTime());
            //窗口起始时间
            String bDate = sdf.format(calendar.getTime());
            calendar2.setTime(sdf.parse(curDay));
            //窗口截至时间
            calendar2.add(calendar2.DATE, 2);
            String eDate = sdf.format(calendar2.getTime());
            //3天为窗口
            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("date").format("yyyyMMdd").gte(bDate).lte(eDate);
            ((BoolQueryBuilder) query).filter(queryBuilder1);
            //按账户分桶
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no")
                    .subAggregation(AggregationBuilders.sum("sum_org_amt").field("org_amt"))
                    .subAggregation(AggregationBuilders.count("count_part_acc_no").field("part_acc_no"))
                    .subAggregation(AggregationBuilders.count("count_three_part_acc_no").field("part_acc_no"));

            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("sum_org_amt", "sum_org_amt");
            bucketsPath.put("count_part_acc_no", "count_part_acc_no");
            bucketsPath.put("count_three_part_acc_no", "count_three_part_acc_no");
            //三日原币种交易金额 >= 1000 且 三日交易笔数是100的整数倍的同时要 >= 三日总交易笔数的60%
            Script script = new Script("params.count_part_acc_no % 100 == 0 && params.sum_org_amt >= 1000 && params.count_three_part_acc_no >= params.count_part_acc_no * 0.6");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_self_acc_no.subAggregation(bs);
            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(1000));
            searchSourceBuilder.query(query);
            searchSourceBuilder.aggregation(agg_self_acc_no);
            searchSourceBuilder.size(0);


            searchRequest.source(searchSourceBuilder);
            System.out.println("查询条件：" + searchSourceBuilder.toString());
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);

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
        }
    }
    /**
     * 计算周期：三日（交易日期）
     * 通过表tb_acc_txn中
     * 字段：
     * Part_bank_name：交易对方行名称
     * 计算三日交易对方行名称为：邮储银行、农业银行、信用社的交易笔数
     * 计算三日总交易笔数
     * 三日交易对方行名称为：邮储银行、农业银行、信用社的交易笔数≥三日累计总交易笔数*50%
     * 三日交易对方行名称为：邮储银行、农业银行、信用社的交易金额≥500000
     **/
    @Test
    public void rule_8_test() throws IOException, ParseException{
        String[] min_max = get_Min_Max("tb_acc_txn", "date2",null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));

        Calendar calendar2 = new GregorianCalendar();

        SearchRequest searchRequest = new SearchRequest("tb_acc_txn");

        for (int i=0;i<daysBetween;i++) {

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            calendar.add(calendar.DATE, 1);
            //当前时间
            String curDay = sdf.format(calendar.getTime());
            //窗口起始时间
            String bDate = sdf.format(calendar.getTime());
            calendar2.setTime(sdf.parse(curDay));
            //窗口截至时间
            calendar2.add(calendar2.DATE, 2);
            String eDate = sdf.format(calendar2.getTime());
            //3天为窗口
            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("date2").format("yyyy-MM-dd").gte(bDate).lte(eDate);
            ((BoolQueryBuilder) query).filter(queryBuilder1);

            //按照账户分桶
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no");
            TermsAggregationBuilder agg_part_bank_name = AggregationBuilders.terms("agg_part_bank_name").field("part_bank_name")
                            .subAggregation(AggregationBuilders.sum("sum_rmb_amt").field("rmb_amt"))
                            .subAggregation(AggregationBuilders.count("count_part_bank_name").field("part_bank_name"));
            agg_self_acc_no.subAggregation(agg_part_bank_name);
            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(1000));
            searchSourceBuilder.aggregation(agg_self_acc_no);
            searchSourceBuilder.query(query);
            searchRequest.source(searchSourceBuilder);

            System.out.println("查询条件：" + searchSourceBuilder.toString());
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);

            Aggregations aggregations = searchResponse.getAggregations();

            ParsedTerms txn_per_day = aggregations.get("agg_self_acc_no");
            // 获取到分组后的所有bucket
            List<? extends Terms.Bucket> buckets = txn_per_day.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                // 解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();

                ParsedTerms per_bank_name = bucketAggregations.get("agg_part_bank_name");
                ParsedTopHits topHits1 = bucketAggregations.get("topHits");
                int len_per_user = topHits1.getHits().getHits().length;

                List<? extends Terms.Bucket> buckets1 = per_bank_name.getBuckets();
                int youchu_count =0; //邮储银行交易次数
                double youchu_money = 0;//邮储银行交易金额
                int nongye_count = 0;//农业银行交易次数
                double nongye_money = 0;//农业银行交易金额
                int xinyongshe_count = 0;//信用社银行交易次数
                double xinyongshe_money = 0;//信用社银行交易金额
                int total_transaction_count = 0;//总交易次数
                for (Terms.Bucket bucket1 : buckets1) {
                    Aggregations bucketAggregations1 = bucket1.getAggregations();
                    Sum sum = bucketAggregations1.get("sum_rmb_amt");
                    ValueCount count = bucketAggregations1.get("count_part_bank_name");
                    total_transaction_count += count.value();
                    if(bucket1.getKeyAsString().contains("邮储")){
                        youchu_count += count.value();
                        youchu_money += sum.value();
                    }
                    else if(bucket1.getKeyAsString().contains("农业")){
                        nongye_count += count.value();
                        nongye_money += sum.value();
                    }
                    else if(bucket1.getKeyAsString().contains("信用社")){
                        xinyongshe_count += count.value();
                        xinyongshe_money += sum.value();
                    }
                    else{
                        continue;
                    }
                }

                if(youchu_count + nongye_count + xinyongshe_count >= total_transaction_count*0.5 && youchu_money + nongye_money + xinyongshe_money >= 500000){
                    System.out.println("邮储银行交易次数："+youchu_count);
                    System.out.println("邮储银行交易金额："+youchu_count);
                    System.out.println("农业银行交易次数："+nongye_count);
                    System.out.println("农业银行交易金额："+nongye_money);
                    System.out.println("信用社交易次数："+xinyongshe_count);
                    System.out.println("信用社交易金额："+xinyongshe_money);
                    System.out.println("总交易次数："+total_transaction_count);
//                    for (int j = 0; j < len_per_user; j++) {
//                        Map<String, Object> sourceAsMap = topHits1.getHits().getHits()[j].getSourceAsMap();
//                        // 打印出统计后的数据
//                        System.out.println(sourceAsMap);
//
//                    }
                }
                else{
                    System.out.println("该账户无符合条件记录。");
                }
            }
        }
    }

    @Test
    public void rule_10_test() throws IOException, ParseException{
        String[] min_max = get_Min_Max("tb_acc_txn", "date2",null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));
        SearchRequest searchRequest = new SearchRequest("tb_acc_txn");
        for (int i = 0; i < daysBetween; i++) {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            calendar.add(calendar.DATE, 1);
            String curDay = sdf.format(calendar.getTime());

            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
            //过滤日期
            queryBuilder.filter(QueryBuilders.termQuery("date2", curDay));

            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no")
                    .subAggregation(AggregationBuilders.sum("sum_rmb_amt").field("rmb_amt"));

            //按交易账户数量大于等于3过滤
            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("sum_rmb_amt", "sum_rmb_amt");
            //判断每个账户的总交易金额的结尾是否为吉利数
            Script script = new Script("(params.sum_rmb_amt - 666)%1000 == 0 || (params.sum_rmb_amt - 888)%1000 == 0 || (params.sum_rmb_amt - 99)%100 == 0 ");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_self_acc_no.subAggregation(bs);
            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(1000));
            searchSourceBuilder.query(queryBuilder);
            searchSourceBuilder.aggregation(agg_self_acc_no);
            searchSourceBuilder.size(0);

            searchRequest.source(searchSourceBuilder);
//            System.out.println("查询条件：" + searchSourceBuilder.toString());
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//            System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);

            Aggregations aggregations = searchResponse.getAggregations();

            ParsedTerms txn_per_day = aggregations.get("agg_self_acc_no");
            // 获取到分组后的所有bucket
            List<? extends Terms.Bucket> buckets = txn_per_day.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                // 解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();
                Sum sum = bucketAggregations.get("sum_rmb_amt");
                System.out.println(sum.getValueAsString());
                ParsedTopHits topHits = bucketAggregations.get("topHits");

                int len = topHits.getHits().getHits().length;
                for (int j = 0; j < len; j++) {
                    Map<String, Object> sourceAsMap = topHits.getHits().getHits()[j].getSourceAsMap();
                    // 打印出统计后的数据
                    System.out.println(sourceAsMap);
                }
            }
        }
    }
}
