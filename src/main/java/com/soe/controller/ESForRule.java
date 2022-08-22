package com.soe.controller;

import com.soe.utils.IpUtil;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.aggregations.pipeline.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


@RestController
public class ESForRule {

    private final RestHighLevelClient restHighLevelClient;


    @Autowired
    public ESForRule(RestHighLevelClient restHighLevelClient){
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
//    @Test
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
    @GetMapping("/rule_1")
    public List<String> rule_1() throws IOException, ParseException {

        List<String> list = new ArrayList<>();
        String[] min_max = get_Min_Max("tb_acc_txn", "date2",null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));
        for (int i=0;i<daysBetween;i++) {
            SearchRequest searchRequest = new SearchRequest("tb_acc_txn");

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        QueryBuilder query = QueryBuilders.matchQuery("Lend_flag","11");
            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            calendar.add(calendar.DATE, 1);
// 这个时间就是日期往后推一天的结果
            String curDay = sdf.format(calendar.getTime());
//            System.out.println(curDay);
//        一天为窗口
//            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("date2").format("yyyy-MM-dd").gte(curDay).lte(curDay);
            QueryBuilder queryBuilder1 = QueryBuilders.termQuery("date2",curDay);
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
            Script script = new Script("params.count_self_bank_code >= 3 && params.sum_rmb_amt >= 500000");
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
                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                String r_date = (String) sourceAsMap.get("date2");
//                折人民币交易金额-收
                String r_lend1 = "0";
//                折人民币交易金额-付
                Sum r_lend2 = bucketAggregations.get("sum_rmb_amt");
//                客户号
                String r_cst_no = (String) sourceAsMap.get("cst_no");
//                客户名称
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
//                网点数量
                ParsedCardinality count_self_bank_code =  bucketAggregations.get("count_self_bank_code");
                String record = "日期="+r_date+", "+"客户号="+r_cst_no+", "+"客户名称="+r_self_acc_name+", 折人民币交易金额-收="+r_lend1+", 折人民币交易金额-付="+r_lend2.getValueAsString()+", 交易笔数收=0 , 交易笔数付="+len;
                list.add(record);
//                for (int j = 0; j < len; j++) {
//                    Map<String, Object> sourceAsMap = topHits.getHits().getHits()[j].getSourceAsMap();
//                    // 打印出统计后的数据
//                    System.out.println(sourceAsMap);
//
//                }


            }
//            restHighLevelClient.close();

        }

//        }
        return list;
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
    @GetMapping("/rule_2")
    public List<String> rule_2() throws IOException, ParseException {
//        File file = new File("result.txt");
//
//        if (!file.exists()) {
//            file.createNewFile();
//        }
//        FileWriter fw = new FileWriter(file.getAbsoluteFile());
//        BufferedWriter bw = new BufferedWriter(fw);
//

        List<String> list = new ArrayList<>();
        String[] min_max = get_Min_Max("tb_acc", "open_time",QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("agent_no","@N")));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));

        Calendar calendar2 = new GregorianCalendar();

        boolean flag = false;
//        3天的窗口可能含有重复结果
//        key:代理人身份证号    value:预警日期
        HashMap<String, String> result = new HashMap<String, String>();
        SearchRequest searchRequest = new SearchRequest("tb_acc");

        for (int i=0;i<daysBetween;i++) {

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
//            QueryBuilder queryBuilder3 = QueryBuilders.wildcardQuery("agent_no","*");
//            ((BoolQueryBuilder) query).filter(queryBuilder3);
////            //agent_name不能为空
//            QueryBuilder queryBuilder4 = QueryBuilders.wildcardQuery("agent_name","*");
//            ((BoolQueryBuilder) query).filter(queryBuilder4);
//
//        //agent_no不能为空
//            QueryBuilder queryBuilder3 = QueryBuilders.termQuery("agent_no","@N");
//            ((BoolQueryBuilder) query).mustNot(queryBuilder3);
////            //agent_name不能为空
//            QueryBuilder queryBuilder4 = QueryBuilders.termQuery("agent_name","@N");
//            ((BoolQueryBuilder) query).mustNot(queryBuilder4);

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
            if(!flag){
                System.out.println(searchSourceBuilder.toString());
                flag = true;
            }
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
                boolean isNew = true;
                if(result.containsKey(r_agent_no)){
                    String exist_date = result.get(r_agent_no);
                    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
                    if(daysBetween(sdf.parse(r_date),sdf.parse(exist_date))>=3){
//                        更新value
                        result.put(r_agent_no,r_date);
                    }else {
                        isNew = false;
                    }
                }else {
                    result.put(r_agent_no,r_date);
                }
                if(isNew)
                {
                    String record = "日期="+r_date+", "+"客户号="+r_cst_no+", "+"客户名称="+r_self_acc_name+", 代理人身份证="+r_agent_no+", 开户数量="+count_self_acc_no.getValueAsString();
                    list.add(record);
//                    System.out.println(record);
                }

            }
//            restHighLevelClient.close();

        }

//        }
    return list;
    }


    /**
     * "计算周期：每日（开户日期）
     * 通过表tb_cst_pers中
     * 字段：
     * Contact1：联系方式
     * Contact2：其他联系方式1
     * Contact3：其他联系方式2
     * 日累计相同联系方式的客户数量进行条件过滤
     * （客户的3个联系方式中的任意一个联系与另外一个客户的3个联系方式中的任意一个相同，都算相同的联系方式）"
     *
     * SELECT * FROM tb_cst_pers WHERE Contact1 != Contact1 and Contact2 != "@N";
     * 返回值为0 即不存在有客户Contact1 ！= Contact2
     * 可以认为每个开户卡号的预留手机号都是唯一的
     * @throws IOException
     * @throws ParseException
     */
    @GetMapping("/rule_3")
    public List<String> rule_3() throws IOException, ParseException {

        List<String> list = new ArrayList<>();

        String[] min_max = get_Min_Max("tb_cst_pers", "open_time",null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));
        SearchRequest searchRequest = new SearchRequest("tb_cst_pers");

        for (int i=0;i<daysBetween;i++) {

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        QueryBuilder query = QueryBuilders.matchQuery("Lend_flag","11");
            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            calendar.add(calendar.DATE, 1);
// 这个时间就是日期往后推一天的结果
            String curDay = sdf.format(calendar.getTime());
//            System.out.println(curDay);
//        一天为窗口
//            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("date2").format("yyyy-MM-dd").gte(curDay).lte(curDay);
            QueryBuilder queryBuilder1 = QueryBuilders.termQuery("open_time",curDay);
            ((BoolQueryBuilder) query).filter(queryBuilder1);

            //        //Contact不能为空
            QueryBuilder queryBuilder3 = QueryBuilders.termQuery("contact1","@N");
            ((BoolQueryBuilder) query).mustNot(queryBuilder3);

            QueryBuilder queryBuilder2 = QueryBuilders.termQuery("contact1","0");
            ((BoolQueryBuilder) query).mustNot(queryBuilder2);
//

//


            //嵌套子聚合查询  以self_acc_name账户名称分桶
            TermsAggregationBuilder agg_self_acc_name = AggregationBuilders.terms("agg_contact1").field("contact1")
                    .subAggregation(AggregationBuilders.cardinality("count_cst_no").field("cst_no"));
//                    .subAggregation(AggregationBuilders.count("sum_rmb_amt").field("rmb_amt"));

            //子聚合  管道聚合不能包含子聚合，但是某些类型的管道聚合可以链式使用（比如计算导数的导数）。
            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("count_cst_no", "count_cst_no");
//            bucketsPath.put("sum_rmb_amt","sum_rmb_amt");
            Script script = new Script("params.count_cst_no >= 2");
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

            ParsedTerms txn_per_day = aggregations.get("agg_contact1");
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

//                    Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                    String r_date = (String) sourceAsMap.get("open_time");
                    String r_contact1 = (String) sourceAsMap.get("contact1");
                    String r_cst_no = (String) sourceAsMap.get("cst_no");
                    String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
                    ParsedCardinality count_cst_no =  bucketAggregations.get("count_cst_no");
                    String record = "日期="+r_date+", "+"客户号="+r_cst_no+", "+"客户名称="+r_self_acc_name+", 联系方式="+r_contact1+", 重复数="+count_cst_no.getValueAsString();
                    list.add(record);
                }


            }
//            restHighLevelClient.close();

        }

//        }
        return list;
    }

    /**
     * "计算周期：每日（交易日期）
     * 通过表tb_acc_txn中
     * 字段：
     * Time：交易时间=00:00至06:00
     * Tsf_flag=11：转账
     * 日累计凌晨转账笔数≥3笔
     * 日累计凌晨转账交易金额≥500000
     * 进行条件过滤"
     * @return
     * @throws IOException
     * @throws ParseException
     */
    @GetMapping("/rule_4")
    public List<String> rule_4() throws IOException, ParseException {

        List<String> list = new ArrayList<>();
        String[] min_max = get_Min_Max("tb_acc_txn", "date2",null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));
        for (int i=0;i<daysBetween;i++) {
            SearchRequest searchRequest = new SearchRequest("tb_acc_txn");

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        QueryBuilder query = QueryBuilders.matchQuery("Lend_flag","11");
            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            calendar.add(calendar.DATE, 1);
// 这个时间就是日期往后推一天的结果
            String curDay = sdf.format(calendar.getTime());
//            System.out.println(curDay);
//        一天为窗口
//            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("date2").format("yyyy-MM-dd").gte(curDay).lte(curDay);
            QueryBuilder queryBuilder1 = QueryBuilders.termQuery("date2",curDay);
            ((BoolQueryBuilder) query).filter(queryBuilder1);
            //Time：交易时间=00:00至06:00
            QueryBuilder queryBuilder2 = QueryBuilders.rangeQuery("time").format("HHmmss").gte("000000").lte("060000");
            ((BoolQueryBuilder) query).filter(queryBuilder2);

//        //Tsf_flag=11：转账
            QueryBuilder queryBuilder3 = QueryBuilders.termQuery("tsf_flag", "11");
            ((BoolQueryBuilder) query).filter(queryBuilder3);
//


            //嵌套子聚合查询  以self_acc_name账户名称分桶
            TermsAggregationBuilder agg_self_acc_name = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no")
                    .subAggregation(AggregationBuilders.count("count_self_acc_no").field("self_acc_no"))
                    .subAggregation(AggregationBuilders.sum("sum_rmb_amt").field("rmb_amt"));

            //子聚合  管道聚合不能包含子聚合，但是某些类型的管道聚合可以链式使用（比如计算导数的导数）。
            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("count_self_acc_no", "count_self_acc_no");
            bucketsPath.put("sum_rmb_amt","sum_rmb_amt");
            Script script = new Script("params.count_self_acc_no >= 3 && params.sum_rmb_amt >= 500000");
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
                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                String r_date = (String) sourceAsMap.get("date2");
//                客户号
                String r_cst_no = (String) sourceAsMap.get("cst_no");
//                客户名称
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
                //总交易笔数
                ParsedValueCount count_self_acc_no =  bucketAggregations.get("count_self_acc_no");
                //收款总金额
                double lend1_amt = 0;
                //收款交易笔数
                int lend1_count =0 ;
                //付款总金额
                double lend2_amt = 0;
                //付款交易笔数
                int lend2_count = 0;

                for (int j = 0; j < len; j++) {
                    Map<String, Object> sourceAsMap2 = topHits.getHits().getHits()[j].getSourceAsMap();
                    //根据Lend_flag判断 收 / 付
                    String lend_flag = (String) sourceAsMap2.get("lend_flag");
                    if(lend_flag.equals("10")){ //收
                        lend1_amt = lend1_amt + (Double) sourceAsMap2.get("rmb_amt");
                        lend1_count++;
                    }else if(lend_flag.equals("11")){//付
                        lend2_amt = lend2_amt + (Double) sourceAsMap2.get("rmb_amt");
                        lend2_count++;
                    }

                }
                String record = "日期="+r_date+", "+"客户号="+r_cst_no+", "+"客户名称="+r_self_acc_name+", 折人民币交易金额-收="+lend1_amt+", 折人民币交易金额-付="+lend2_amt+", 交易笔数收="+lend1_count+" , 交易笔数付="+lend2_count+", 总交易笔数="+count_self_acc_no.getValueAsString();
                list.add(record);

            }
//            restHighLevelClient.close();

        }

//        }
        return list;
    }

    @GetMapping("/update_tb_acc_txt_Nation")
    public void update_tb_acc_txt_Nation() throws IOException {
        int len =22000000; //设初赛数据量
        int unit = 10000;// 一次查询10000
        int step = len/unit;

        SearchRequest searchRequest = new SearchRequest("tb_acc_txn");//指定搜索索引
        for(int i=0;i<step;i++){
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();//指定条件对象
            QueryBuilder query = QueryBuilders.boolQuery();
            QueryBuilder queryBuilder1 = QueryBuilders.termQuery("bord_flag","11");
//        ((BoolQueryBuilder) query).filter(queryBuilder1);
            //Time：交易时间=00:00至06:00
            QueryBuilder queryBuilder2 = QueryBuilders.termQuery("ip_code","@N");
            ((BoolQueryBuilder) query).mustNot(queryBuilder2);

            QueryBuilder queryBuilder3 = QueryBuilders.termQuery("ip_code","");
            ((BoolQueryBuilder) query).mustNot(queryBuilder3);
            sourceBuilder.fetchSource(new String[]{"ip_code"}, new String[]{});
            //已赋值的
//            QueryBuilder queryBuilder4 = QueryBuilders.termQuery("nation","@N");
//        ((BoolQueryBuilder) query).filter(queryBuilder4);
//        sourceBuilder.query(QueryBuilders.boolQuery()
//                //查询条件  Bord_flag：跨境交易标识 11
//                .filter(QueryBuilders.termQuery("bord_flag","11")).mustNot(QueryBuilders.termQuery("ip_code","@N"))).fetchSource(new String[]{"ip_code"}, new String[]{});
//
            sourceBuilder.query(query);
            sourceBuilder.from(i*unit);
            sourceBuilder.size(unit);
            //进度
            System.out.println(i*unit);
            searchRequest.source(sourceBuilder);//指定查询条件

            RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
            builder.setHttpAsyncResponseConsumerFactory(
                    new HttpAsyncResponseConsumerFactory
                            //修改为5000MB
                            .HeapBufferedResponseConsumerFactory(5000 * 1024 * 1024));
            RequestOptions requestOptions=builder.build();

            //参数1：搜索的请求对象，   参数2：请求配置对象   返回值：查询结果对象
//            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, requestOptions);
//            System.out.println("总条数："+searchResponse.getHits().getTotalHits().value);
            //获取结果
            SearchHit[] hits = searchResponse.getHits().getHits();
            if(searchResponse.getHits().getTotalHits().value>0){
                for(SearchHit hit:hits){
                    String id = hit.getId();
//            System.out.println("id: "+id+"  source: "+hit.getSourceAsString());
                    UpdateRequest updateRequest = new UpdateRequest("tb_acc_txn", id);

                    Map<String, Object> kvs = new HashMap<>();
//                    System.out.println(hit.getSourceAsMap().get("ip_code"));
                    String nation = IpUtil.getNationByIP((String) hit.getSourceAsMap().get("ip_code"));
//                    System.out.println(nation);
                    kvs.put("nation", nation);
                    updateRequest.doc(kvs);
//            updateRequest.timeout(TimeValue.timeValueSeconds(1));
//            updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
                    //更新
                    restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
                }
            }
        }
        System.out.println("更新完成");


    }


    /**
     * "计算周期：三日（交易日期）
     * 通过表tb_acc_txn中
     * 字段：
     * Bord_flag：跨境交易标识
     * Nation：交易对方所在国家或地区
     * 计算三日交易跨境交易标识=11的交易根据ip地址库判断交易对方所在国家或地区
     * 三日交易对方所在国家或地区≥10
     * 进行条件过滤"
     * @return
     * @throws IOException
     * @throws ParseException
     */
    @GetMapping("/rule_12")
    public List<String> rule_12() throws IOException, ParseException {

        List<String> list = new ArrayList<>();

        String[] min_max = get_Min_Max("tb_acc_txn", "date2",QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("bord_flag","11")));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));

        Calendar calendar2 = new GregorianCalendar();

        boolean flag = false;
//        3天的窗口可能含有重复结果
//        key:代理人身份证号    value:预警日期
        HashMap<String, String> result = new HashMap<String, String>();
        SearchRequest searchRequest = new SearchRequest("tb_acc_txn");

        for (int i=0;i<daysBetween;i++) {

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
            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("date2").format("yyyy-MM-dd").gte(bDate).lte(eDate);
            ((BoolQueryBuilder) query).filter(queryBuilder1);

            QueryBuilder queryBuilder2 = QueryBuilders.termQuery("bord_flag","11");
        ((BoolQueryBuilder) query).filter(queryBuilder2);

            //嵌套子聚合查询  以Self_acc_no分桶
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no")
                    .subAggregation(AggregationBuilders.cardinality("count_nation").field("nation")); //跨境nation数量


            //子聚合  管道聚合不能包含子聚合
            Map<String, String> bucketsPath = new HashMap<>();
//            bucketsPath.put("agg_self_acc_no", "agg_self_acc_no");
            bucketsPath.put("count_nation", "count_nation");
            Script script = new Script("params.count_nation >= 2");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_self_acc_no.subAggregation(bs);

            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(1000));
            searchSourceBuilder.aggregation(agg_self_acc_no);
            searchSourceBuilder.size(0);


            searchSourceBuilder.query(query);

            searchRequest.source(searchSourceBuilder);
//            System.out.println("查询条件：" + searchSourceBuilder.toString());
            if(!flag){
                System.out.println(searchSourceBuilder.toString());
                flag = true;
            }
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
                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                String r_date = (String) sourceAsMap.get("date2");
                String r_nation = (String) sourceAsMap.get("nation");
                String r_cst_no = (String) sourceAsMap.get("cst_no");
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
                ParsedCardinality count_nation =  bucketAggregations.get("count_nation");
//                String record = "日期="+r_date+", "+"客户号="+r_cst_no+", "+"客户名称="+r_self_acc_name+", nation="+r_nation+", nation数量="+count_nation.getValueAsString();
//                System.out.println(record);

                boolean isNew = true;
                if(result.containsKey(r_cst_no)){
                    String exist_date = result.get(r_cst_no);
                    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
                    if(daysBetween(sdf.parse(r_date),sdf.parse(exist_date))>=3){
//                        更新value
                        result.put(r_cst_no,r_date);
                    }else {
                        isNew = false;
                    }
                }else {
                    result.put(r_cst_no,r_date);
                }
                if(isNew)
                {
                    String record = "日期="+r_date+", "+"客户号="+r_cst_no+", "+"客户名称="+r_self_acc_name+", nation="+r_nation+", nation数量="+count_nation.getValueAsString();
                    list.add(record);
                    System.out.println(record);
                }

            }
//            restHighLevelClient.close();

        }

//        }
        return list;
    }


}
