package com.soe.caiqihang;


import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
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


@SpringBootTest
public class ESForRule16_20 {
    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient restHighLevelClient;

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
     * 传入index和字段field，查询该index内 该field的最小值和最大值
     * 如：用于获取日期的最小值和最大值，以便按时间窗口轮询
     * @param index
     * @param field
     * @return
     */
    public String[] get_Min_Max(String index, String field, QueryBuilder queryBuilder){
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

    //输入两个日期字符串，输出年龄 如：19991225-->22
    public int age(String  now, String birth) throws ParseException {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date time1 = sdf.parse(now);
        Date time2 = sdf.parse(birth);
        int yearnow = time1.getYear();
        int monthnow = time1.getMonth();
        int yearbirth = time2.getYear();
        int monthbirth = time2.getMonth();
        int age = yearnow - yearbirth;
        if(monthnow<monthbirth) age--;
        return age;
    }

    /**
     * 计算周期：三日（交易日期）
     * 通过表tb_acc_txn中
     * 字段：
     * Org_amt：原币种交易金额
     * Part_acc_no：交易对手卡号
     * 计算三日交易金额≥10000且交易金额是10000的整数倍的整万的主体
     * 计算该主体交易对手卡号重复≥3次
     * 进行条件过滤
     */
    //问题：规则16和规则20都是需要处理子桶内数量
    @Test
    public void rule_16_test() throws ParseException, IOException {
        //获取最大和最小日期范围
        String[] min_max = get_Min_Max("tb_acc_txn","date2",null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));

        Calendar calendar2 = new GregorianCalendar();

        SearchRequest searchRequest = new SearchRequest("tb_acc_txn");

        for(int i = 0; i < daysBetween; i++){

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            calendar.add(calendar.DATE,1);
            //当前时间
            String curDay = sdf.format(calendar.getTime());
            //窗口起始时间
            String bDate = sdf.format(calendar.getTime());
            calendar2.setTime(sdf.parse(curDay));
            //窗口截止时间
            calendar2.add(calendar2.DATE,2);
            String eDate = sdf.format(calendar2.getTime());
            //3天为窗口
            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("date2").format("yyyy-MM-dd").gte(bDate).lte(eDate);
            ((BoolQueryBuilder) query).filter(queryBuilder1);
            //按账户分桶
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no")
                    .subAggregation(AggregationBuilders.sum("sum_org_amt").field("org_amt"))
                    .subAggregation(AggregationBuilders.terms("agg_part_acc_no").field("part_acc_no"));

            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("sum_org_amt","sum_org_amt");
            bucketsPath.put("count","_count");
            //三日交易金额≥10000 且 交易金额是10000的整数倍的整万
            Script script = new Script("params.sum_org_amt >= 10000 && params.sum_org_amt %10000 == 0 && params.count >= 3");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_self_acc_no.subAggregation(bs);
            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(100));
            sourceBuilder.query(query);
            sourceBuilder.aggregation(agg_self_acc_no);
            sourceBuilder.size(0);

            searchRequest.source(sourceBuilder);
            System.out.println("查询条件："+sourceBuilder.toString());
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println("总条数："+searchResponse.getHits().getTotalHits().value);

            //处理聚合结果
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedStringTerms parsedStringTerms = aggregations.get("agg_self_acc_no");
            //获取分组后的所有bucket
            List<? extends Terms.Bucket> buckets = parsedStringTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                //解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();
                ParsedStringTerms parsedStringTerms1 = aggregations.get("agg_part_acc_no");
                ParsedTopHits topHits = bucketAggregations.get("topHits");
                SearchHit[] hits = searchResponse.getHits().getHits();
                int len = topHits.getHits().getHits().length;
                //System.out.println(len);
                List<? extends Terms.Bucket> buckets1 = parsedStringTerms1.getBuckets();
                for (Terms.Bucket bucket1 : buckets1) {
                    System.out.println(bucket1.getDocCount());
                    for (int j = 0; j < len; j++) {
                        Map<String, Object> sourceAsMap = topHits.getHits().getHits()[j].getSourceAsMap();
                        // 打印出统计后的数据
                        System.out.println(sourceAsMap);

                    }
                }
            }
        }
    }

    /**
     * 计算周期：三日（开户日期）
     * 通过表tb_acc中
     * 字段：
     * Agent_name：代理人姓名
     * Agent_no：代理人证件号
     * Id_no：身份证件号码
     * 计算三日Agent_name代理人姓名+Agent_no代理人证件号相同的主体
     * 截取该主体证件号码的日期进行年龄的计算（年龄小于18或大于70）
     * 进行条件过滤
     */
    @Test
    public void rule_17_test() throws ParseException, IOException {
        //获取最大和最小日期范围,只取具有代理关系的
        String[] min_max = get_Min_Max("tb_acc","open_time",QueryBuilders.boolQuery().must(QueryBuilders.termQuery("open_type1","11")));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));

        Calendar calendar2 = new GregorianCalendar();

        SearchRequest searchRequest = new SearchRequest("tb_acc");

        for(int i = 0; i < daysBetween; i++){

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            calendar.add(calendar.DATE,1);
            //当前时间
            String curDay = sdf.format(calendar.getTime());
            //窗口起始时间
            String bDate = sdf.format(calendar.getTime());
            calendar2.setTime(sdf.parse(curDay));
            //窗口截止时间
            calendar2.add(calendar2.DATE,2);
            String eDate = sdf.format(calendar2.getTime());
            //3天为窗口
            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("open_time").format("yyyyMMdd").gte(bDate).lte(eDate);
            ((BoolQueryBuilder) query).filter(queryBuilder1);

            //按代理人分桶
            TermsAggregationBuilder agg_agent_no = AggregationBuilders.terms("agg_agent_no").field("agent_no");

            agg_agent_no.subAggregation(AggregationBuilders.topHits("topHits").size(100));
            sourceBuilder.query(query);
            sourceBuilder.aggregation(agg_agent_no);
            sourceBuilder.size(0);

            searchRequest.source(sourceBuilder);
            System.out.println("查询条件："+sourceBuilder.toString());
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println("总条数："+searchResponse.getHits().getTotalHits().value);

            //处理聚合结果
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedStringTerms parsedStringTerms = aggregations.get("agg_agent_no");
            //获取分组后的所有bucket
            List<? extends Terms.Bucket> buckets = parsedStringTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                //解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();
                ParsedTopHits topHits = bucketAggregations.get("topHits");
                int len = topHits.getHits().getHits().length;
                //问题：  这里我不会取到open_time和id_no两个字段的值
                String open_time = bucketAggregations.get("open_time").toString();
                String birthday = bucketAggregations.get("id_no").toString().substring(6,14);
                int age = age(open_time,birthday);
                if(age<18 || age>70){
                    System.out.println(age);
                }
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
     * Nation：交易对方国家地区
     * 计算三日Nation重复≥5的主体
     * 进行条件过滤
     */
    @Test
    public void rule_20_test() throws IOException, ParseException {
        //获取最大和最小日期范围
        String[] min_max = get_Min_Max("tb_acc_txn","date2",null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));

        Calendar calendar2 = new GregorianCalendar();

        SearchRequest searchRequest = new SearchRequest("tb_acc_txn");

        for(int i = 0; i < daysBetween; i++){
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            calendar.add(calendar.DATE,1);
            //当前时间
            String curDay = sdf.format(calendar.getTime());
            //窗口起始时间
            String bDate = sdf.format(calendar.getTime());
            calendar2.setTime(sdf.parse(curDay));
            //窗口截止时间
            calendar2.add(calendar2.DATE,2);
            String eDate = sdf.format(calendar2.getTime());
            //3天为窗口
            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("date2").format("yyyy-MM-dd").gte(bDate).lte(eDate);
            ((BoolQueryBuilder) query).filter(queryBuilder1);
            //bord_flag=11,观察后发现nation有值的数据，bord_flag不一定是11，也有可能是12
            //QueryBuilder queryBuilder2 = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("bord_flag","11"));
            //((BoolQueryBuilder) query).filter(queryBuilder2);
            //按客户号分桶
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no")
                    .subAggregation(AggregationBuilders.terms("agg_nation").field("nation"));

            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("count","_count");
            //三日Nation交易对方国家地区重复≥5的主体
            Script script = new Script("params.count>=5");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_self_acc_no.subAggregation(bs);
            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(100));
            sourceBuilder.query(query);
            sourceBuilder.aggregation(agg_self_acc_no);
            sourceBuilder.size(0);
            searchRequest.source(sourceBuilder);
            System.out.println("查询条件："+sourceBuilder.toString());
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println("总条数："+searchResponse.getHits().getTotalHits().value);
            //处理聚合结果
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms per_name = aggregations.get("agg_self_acc_no");
            //获取分组后的所有bucket
            List<? extends Terms.Bucket> buckets = per_name.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                //解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();

                ParsedTerms per_nation = aggregations.get("agg_nation");
                ParsedTopHits topHits = bucketAggregations.get("topHits");
                int len = topHits.getHits().getHits().length;
                List<? extends Terms.Bucket> buckets1 = per_nation.getBuckets();
                for (Terms.Bucket bucket1 : buckets1) {
                    System.out.println(bucket1.getKey()+" "+bucket1.getDocCount());
                    for (int j = 0; j < len; j++) {
                        Map<String, Object> sourceAsMap = topHits.getHits().getHits()[j].getSourceAsMap();
                        // 打印出统计后的数据
                        System.out.println(sourceAsMap);
                    }
                }
            }
        }
    }


}


