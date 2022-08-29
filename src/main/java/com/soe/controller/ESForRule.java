package com.soe.controller;

import com.soe.utils.CsvUtil;
//import com.soe.utils.IpUtil;
import lombok.SneakyThrows;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import static com.soe.utils.ESUtils.daysBetween;


@RestController
public class ESForRule {

    private final RestHighLevelClient restHighLevelClient;
    //结果集csv文件的标题行
    private final String headDataStr ="规则代码,预警日期,客户号,客户名称,折人民币交易金额-收,折人民币交易金额-付,交易笔数收,交易笔数付";
    //最终结果csv文件路径
    private final String csvfile = "./result/result.csv";

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
    @GetMapping("rule_1")
    @Async
    public void rule_1() throws IOException, ParseException {
        System.out.println("rule_1 : begin");

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
            //第一次不加1
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
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
                    .subAggregation(AggregationBuilders.sum("sum_org_amt").field("org_amt"));

            //子聚合  管道聚合不能包含子聚合，但是某些类型的管道聚合可以链式使用（比如计算导数的导数）。
            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("count_self_bank_code", "count_self_bank_code");
            bucketsPath.put("sum_org_amt","sum_org_amt");
            Script script = new Script("params.count_self_bank_code >= 3 && params.sum_org_amt >= 500000");
//            Script script = new Script("params.count_self_bank_code>=3");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_self_acc_name.subAggregation(bs);

            agg_self_acc_name.subAggregation(AggregationBuilders.topHits("topHits").size(30000));
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
//                System.out.println(len);
                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                String r_date = (String) sourceAsMap.get("date2");
//                折人民币交易金额-收
                String r_lend1 = "0";
//                折人民币交易金额-付
                Sum r_lend2 = bucketAggregations.get("sum_org_amt");
//                客户号
                String r_cst_no = (String) sourceAsMap.get("cst_no");
//                客户名称
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
//                网点数量
                ParsedCardinality count_self_bank_code =  bucketAggregations.get("count_self_bank_code");
//                String record = "日期="+r_date+", "+"客户号="+r_cst_no+", "+"客户名称="+r_self_acc_name+", 折人民币交易金额-收="+r_lend1+", 折人民币交易金额-付="+r_lend2.getValueAsString()+", 交易笔数收=0 , 交易笔数付="+len;
                //写入到csv文件，注意各列对其，用英文逗号隔开
                //规则代码,预警日期,客户号,客户名称,折人民币交易金额-收,折人民币交易金额-付,交易笔数收,交易笔数付
                String record = "JRSJ-001,"+r_date+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",r_lend1)+","+String.format("%.2f",r_lend2.getValueAsString())+",0,"+String.valueOf(len);
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
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        System.out.println("rule_1 : end");
//        return list;
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
    @GetMapping("rule_2")
    @Async
    public void rule_2() throws IOException, ParseException {
//        File file = new File("result.txt");
//
//        if (!file.exists()) {
//            file.createNewFile();
//        }
//        FileWriter fw = new FileWriter(file.getAbsoluteFile());
//        BufferedWriter bw = new BufferedWriter(fw);
//
        System.out.println("rule_2 : begin");

        List<String> list = new ArrayList<>();
        String[] min_max = get_Min_Max("tb_acc", "open_time",QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("agent_no","@N")));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");

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
            //第一次不加1
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
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

            agg_agent_no.subAggregation(AggregationBuilders.topHits("topHits").size(30000));
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
//                System.out.println(len);
                //预警日期：为筛出数据里最大交易日期+1天，直接倒序扫描，第一次扫描到的就是最大日期
                for (int j = len - 1; j >= 0; j--) {

                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[j].getSourceAsMap();
                String r_date = (String) sourceAsMap.get("open_time");
                String r_agent_no = (String) sourceAsMap.get("agent_no");
                String r_cst_no = (String) sourceAsMap.get("cst_no");
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
                ParsedCardinality count_self_acc_no = bucketAggregations.get("count_self_acc_no");
//                boolean isNew = true;
//                if (result.containsKey(r_agent_no)) {
//                    String exist_date = result.get(r_agent_no);
////                    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
//                    if (daysBetween(sdf.parse(r_date), sdf.parse(exist_date)) >= 3) {
////                        更新value
//                        result.put(r_agent_no, r_date);
//                    } else {
//                        isNew = false;
//                    }
//                } else {
//                    result.put(r_agent_no, r_date);
//                }
//                if (isNew) {
//                    String record = "日期="+r_date+", "+"客户号="+r_cst_no+", "+"客户名称="+r_self_acc_name+", 代理人身份证="+r_agent_no+", 开户数量="+count_self_acc_no.getValueAsString();
                    //写入到csv文件，注意各列对其，用英文逗号隔开
                    //规则代码,预警日期,客户号,客户名称,折人民币交易金额-收,折人民币交易金额-付,交易笔数收,交易笔数付
                    Calendar calendar3 = new GregorianCalendar();
                    calendar3.setTime(sdf2.parse(sdf2.format(sdf.parse(r_date))));
                    calendar3.add(calendar3.DATE, 1); //预警日期：为筛出数据里最大交易日期+1天
                    String record = "JRSJ-002," + sdf2.format(calendar3.getTime()) + "," + r_cst_no + "," + r_self_acc_name + ",,,,";
                    list.add(record);
//                    System.out.println(record);
//                }
            }

            }
//            restHighLevelClient.close();

        }

//        }
        list = removeDuplicationByHashSet(list);
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        System.out.println("rule_2 : end");
//    return list;
    }

    /**使用HashSet实现List去重(无序)
     *
     * @param list
     * */
    public static List removeDuplicationByHashSet(List<String> list) {
        HashSet set = new HashSet(list);
        //把List集合所有元素清空
        list.clear();
        //把HashSet对象添加至List集合
        list.addAll(set);
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
    @GetMapping("rule_3")
    @Async
    public void rule_3() throws IOException, ParseException {
        System.out.println("rule_3 : begin");

        List<String> list = new ArrayList<>();

        String[] min_max = get_Min_Max("tb_cst_pers", "open_time",null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");

        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));
        SearchRequest searchRequest = new SearchRequest("tb_cst_pers");

        for (int i=0;i<daysBetween;i++) {

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        QueryBuilder query = QueryBuilders.matchQuery("Lend_flag","11");
            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            //第一次不加1
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
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
//                    .subAggregation(AggregationBuilders.count("sum_org_amt").field("org_amt"));

            //子聚合  管道聚合不能包含子聚合，但是某些类型的管道聚合可以链式使用（比如计算导数的导数）。
            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("count_cst_no", "count_cst_no");
//            bucketsPath.put("sum_org_amt","sum_org_amt");
            Script script = new Script("params.count_cst_no >= 2");
//            Script script = new Script("params.count_self_bank_code>=3");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_self_acc_name.subAggregation(bs);

            agg_self_acc_name.subAggregation(AggregationBuilders.topHits("topHits").size(30000));
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

//                System.out.println(len);
                for (int j = 0; j < len; j++) {
                    Map<String, Object> sourceAsMap = topHits.getHits().getHits()[j].getSourceAsMap();
                    // 打印出统计后的数据
//                    System.out.println(sourceAsMap);

//                    Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                    String r_date = (String) sourceAsMap.get("open_time");
                    String r_contact1 = (String) sourceAsMap.get("contact1");
                    String r_cst_no = (String) sourceAsMap.get("cst_no");
                    String r_acc_name = (String) sourceAsMap.get("acc_name");
                    ParsedCardinality count_cst_no =  bucketAggregations.get("count_cst_no");
//                    String record = "日期="+r_date+", "+"客户号="+r_cst_no+", "+"客户名称="+r_acc_name+", 联系方式="+r_contact1+", 重复数="+count_cst_no.getValueAsString();
                    //写入到csv文件，注意各列对其，用英文逗号隔开
                    //规则代码,预警日期,客户号,客户名称,折人民币交易金额-收,折人民币交易金额-付,交易笔数收,交易笔数付
                    String record = "JRSJ-003,"+sdf2.format(sdf.parse(r_date))+","+r_cst_no+","+r_acc_name+",,,,";
                    list.add(record);
                }


            }
//            restHighLevelClient.close();

        }

//        }
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        System.out.println("rule_3 : end");
//        return list;
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
    @GetMapping("rule_4")
    @Async
    public void rule_4() throws IOException, ParseException {
        System.out.println("rule_4 : begin");

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
            //第一次不加1
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
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
//            TermsAggregationBuilder agg_self_acc_name = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no")
            TermsAggregationBuilder agg_self_acc_name = AggregationBuilders.terms("agg_self_acc_no").field("cst_no")
                    .subAggregation(AggregationBuilders.count("count_self_acc_no").field("self_acc_no"))
                    .subAggregation(AggregationBuilders.sum("sum_org_amt").field("org_amt"));

            //子聚合  管道聚合不能包含子聚合，但是某些类型的管道聚合可以链式使用（比如计算导数的导数）。
            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("count_self_acc_no", "count_self_acc_no");
            bucketsPath.put("sum_org_amt","sum_org_amt");
            Script script = new Script("params.count_self_acc_no >= 3 && params.sum_org_amt >= 500000");
//            Script script = new Script("params.count_self_bank_code>=3");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_self_acc_name.subAggregation(bs);

            agg_self_acc_name.subAggregation(AggregationBuilders.topHits("topHits").size(30000));
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
//                System.out.println(len);
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
                        lend1_amt = lend1_amt + (Double) sourceAsMap2.get("org_amt");
                        lend1_count++;
                    }else if(lend_flag.equals("11")){//付
                        lend2_amt = lend2_amt + (Double) sourceAsMap2.get("org_amt");
                        lend2_count++;
                    }

                }
//                String record = "日期="+r_date+", "+"客户号="+r_cst_no+", "+"客户名称="+r_self_acc_name+", 折人民币交易金额-收="+lend1_amt+", 折人民币交易金额-付="+lend2_amt+", 交易笔数收="+lend1_count+" , 交易笔数付="+lend2_count+", 总交易笔数="+count_self_acc_no.getValueAsString();
                //写入到csv文件，注意各列对其，用英文逗号隔开
                //规则代码,预警日期,客户号,客户名称,折人民币交易金额-收,折人民币交易金额-付,交易笔数收,交易笔数付
                String record = "JRSJ-004,"+r_date+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);
                list.add(record);

            }
//            restHighLevelClient.close();

        }

//        }
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        System.out.println("rule_4 : end");
//        return list;
    }

    public List<String> get_low_rate_cst() throws IOException {
        //返回list
        List<String> list = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest("tb_low_rate_cst");//指定搜索索引
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();//指定条件对象
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.size(10000);
        searchRequest.source(sourceBuilder);//指定查询条件

        //参数1：搜索的请求对象，   参数2：请求配置对象   返回值：查询结果对象
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        System.out.println("总条数："+searchResponse.getHits().getTotalHits().value);
        //获取结果
        SearchHit[] hits = searchResponse.getHits().getHits();
        for(SearchHit hit:hits){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String cst_no = (String) sourceAsMap.get("cst_id");
//            System.out.println(cst_no);
            list.add(cst_no);
        }
        return list;
    }


    /**
     * "计算周期：三日（交易日期）
     * 通过表tb_cred_txn中
     * 低费率商户清单
     * 计算三日累计总交易笔数
     * 计算三日累计低费率商户信用卡交易笔数
     * 三日累计低费率商户信用卡交易笔数≥三日累计总交易笔数*60%
     * 三日累计低费率商户信用卡交易金额≥500000
     * 进行条件过滤"
     * @throws IOException
     * @throws ParseException
     */
    @GetMapping("rule_5")
    @Async
    public void rule_5() throws IOException, ParseException {
        List<String> list = new ArrayList<>();
        String[] min_max = get_Min_Max("tb_cred_txn", "date",null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        //输出的csv的时间格式化
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));

        Calendar calendar2 = new GregorianCalendar();

        SearchRequest searchRequest = new SearchRequest("tb_cred_txn");
        //读取低费率商户cst_no
        List<String> low_rate_cst_no_list = get_low_rate_cst();

        for (int i=0;i<daysBetween;i++) {

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            //第一次不加1
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
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
//            System.out.println(bDate+"  "+eDate);
            ((BoolQueryBuilder) query).must(queryBuilder1);

            searchSourceBuilder.query(query);
            searchSourceBuilder.size(10000);

            searchRequest.source(searchSourceBuilder);
            //            System.out.println("查询条件：" + searchSourceBuilder.toString());
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            //            System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);


            //获取命中对象hits
            SearchHits hits = searchResponse.getHits();
            //获取总记录数  ,即该组的总交易笔数
            long len = hits.getTotalHits().value;
//            System.out.println("总记录数："+len);

            //获取hits数组
            SearchHit[] hits1 = hits.getHits();
            //低费率交易次数
            int low_rate_transaction_count = 0;
            //低费率交易总金额
            double low_rate_transaction_rmt = 0;
            //收款总金额
            double lend1_amt = 0;
            //收款交易笔数
            int lend1_count =0 ;
            //付款总金额
            double lend2_amt = 0;
            //付款交易笔数
            int lend2_count = 0;

            //交易时间
            String r_date = "";
            //客户号
            String r_cst_no ="";
            //客户名称
            String r_self_acc_name ="";

            for (SearchHit hit : hits1) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();

                r_date = (String) sourceAsMap.get("date");
                //                客户号
                r_cst_no = (String) sourceAsMap.get("cst_no");
                //                客户名称
                r_self_acc_name = (String) sourceAsMap.get("self_acc_name");

                //本次交易金额
                double amt = 0.0;
                if(!(sourceAsMap.get("org_amt").toString()).equals("0")){
                    amt = (Double) sourceAsMap.get("org_amt");
                }
//                if(sourceAsMap.get("org_amt").equals("0")){
//                    System.out.println(0);
//                }

//                Double rmt = Double.parseDouble((String) sourceAsMap.get("org_amt"));

//                Double org_amt = (Double.parseDouble(sourceAsMap.get("org_amt").toString()));
                if(low_rate_cst_no_list.contains(r_cst_no)){
//                    System.out.println(r_cst_no);
                    low_rate_transaction_rmt+=amt;
                    low_rate_transaction_count++;
                }


                String lend_flag = (String) sourceAsMap.get("lend_flag");
                if(lend_flag.equals("10")){ //收
                    lend1_amt = lend1_amt + amt ;
                    lend1_count++;
                }else if(lend_flag.equals("11")){//付
                    lend2_amt = lend2_amt + amt ;
                    lend2_count++;
                }
            }

            //三日累计低费率商户信用卡交易笔数≥三日累计总交易笔数*60%
            //三日累计低费率商户信用卡交易金额≥500000
            if(low_rate_transaction_count >= len*0.6 && low_rate_transaction_rmt>=500000){
                String record = "JRSJ-005,"+sdf2.format(sdf.parse(r_date))+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);
                list.add(record);
//                System.out.println(record);
            }

        }
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);

        System.out.println("rule_5 : end");
//        return list;
    }



    /**
     * "计算周期：每日
     * 通过表tb_acc_txn中
     * 字段：
     * Part_acc_no：交易对手卡号、账号
     * Lend_flag=10：资金收付为收
     * 日累计交易不同交易对手个数≥3
     **/
    @GetMapping("rule_6")
    @Async
    public void rule_6() throws IOException, ParseException {
        //获取最大和最小日期范围
        List<String> list = new ArrayList<>();
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
            //第一次不加1
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
            String curDay = sdf.format(calendar.getTime());

            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
            //过滤日期
            queryBuilder.filter(QueryBuilders.termQuery("date2", curDay));
            //过滤资金收付为收
            queryBuilder.filter(QueryBuilders.termQuery("lend_flag", "10"));
            //按账户名进行分桶
//            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no")
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("cst_no")
                    .subAggregation(AggregationBuilders.cardinality("count_part_acc_no").field("part_acc_no"))
                    .subAggregation(AggregationBuilders.sum("sum_org_amt").field("org_amt"));

            //按交易账户数量大于等于3过滤
            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("count_part_acc_no", "count_part_acc_no");
            Script script = new Script("params.count_part_acc_no >= 3");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_self_acc_no.subAggregation(bs);

            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(30000));
            searchSourceBuilder.aggregation(agg_self_acc_no);
            searchSourceBuilder.size(0);
            searchSourceBuilder.query(queryBuilder);

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
                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                int len = topHits.getHits().getHits().length;
//                System.out.println(len);
                String r_date = (String) sourceAsMap.get("date2");
//                折人民币交易金额-收
//                Sum r_lend1 = bucketAggregations.get("sum_org_amt");
////                折人民币交易金额-付
//                String r_lend2 = "0";
//                客户号
                String r_cst_no = (String) sourceAsMap.get("cst_no");
//                客户名称
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
                //写入到csv文件，注意各列对其，用英文逗号隔开
                //规则代码,预警日期,客户号,客户名称,折人民币交易金额-收,折人民币交易金额-付,交易笔数收,交易笔数付
                String record = "JRSJ-006,"+r_date+","+r_cst_no+","+r_self_acc_name+",,,,";
                list.add(record);
            }
        }
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        System.out.println("rule_6 : end");
//        return list;

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
    @GetMapping("rule_7")
    @Async
    public void rule_7() throws IOException, ParseException {
        List<String> list = new ArrayList<>();
        String[] min_max = get_Min_Max("tb_cred_txn", "date",null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        //输出的csv的时间格式化
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));

        Calendar calendar2 = new GregorianCalendar();

        SearchRequest searchRequest = new SearchRequest("tb_cred_txn");

        for (int i=0;i<daysBetween;i++) {

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            //第一次不加1
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
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
//            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no");
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("cst_no");

            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(30000));
            searchSourceBuilder.query(query);
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
                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                String r_date = (String) sourceAsMap.get("date");
    //                客户号
                String r_cst_no = (String) sourceAsMap.get("cst_no");
    //                客户名称
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
                //收款总金额
                double lend1_amt = 0;
                //收款交易笔数
                int lend1_count =0 ;
                //付款总金额
                double lend2_amt = 0;
                //付款交易笔数
                int lend2_count = 0;
                int three_days_transaction_count = 0;
                for (int j = 0; j < len; j++) {
                    Map<String, Object> sourceAsMap1 = topHits.getHits().getHits()[j].getSourceAsMap();
                    Double per_org_amt = (Double.parseDouble(sourceAsMap1.get("org_amt").toString()));
                    if(per_org_amt >= 1000 && per_org_amt % 100 == 0){
                        three_days_transaction_count += 1;
                    }
                    String lend_flag = (String) sourceAsMap1.get("lend_flag");
                    if(lend_flag.equals("10")){ //收
                        lend1_amt = lend1_amt + (Double.parseDouble(sourceAsMap1.get("org_amt").toString())) ;
                        lend1_count++;
                    }else if(lend_flag.equals("11")){//付
                        lend2_amt = lend2_amt + (Double.parseDouble(sourceAsMap1.get("org_amt").toString())) ;
                        lend2_count++;
                    }
                }
                if(three_days_transaction_count >= len * 0.6){
                    String record = "JRSJ-007,"+sdf2.format(sdf.parse(r_date))+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);
                    list.add(record);
                }
            }
        }
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        System.out.println("rule_7 : end");
//        return list;
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
    @GetMapping("rule_8")
    @Async
    public void rule_8() throws IOException, ParseException{
        List<String> list = new ArrayList<>();
        BoolQueryBuilder qb = QueryBuilders.boolQuery();
        WildcardQueryBuilder q1 = QueryBuilders.wildcardQuery("part_bank_name", "*邮*");
        WildcardQueryBuilder q2 = QueryBuilders.wildcardQuery("part_bank_name", "*农业*");
        WildcardQueryBuilder q3 = QueryBuilders.wildcardQuery("part_bank_name", "*信用社*");
        qb.should(q1);
        qb.should(q2);
        qb.should(q3);
        String[] min_max = get_Min_Max("tb_acc_txn", "date2",qb);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));

        Calendar calendar2 = new GregorianCalendar();

        SearchRequest searchRequest = new SearchRequest("tb_acc_txn");

        Calendar calendar3 = new GregorianCalendar();
        for (int i=0;i<daysBetween;i++) {
            //        3天的窗口可能含有重复结果
            HashMap<String, String> result = new HashMap<String, String>();
            //该规则结果集去重  key:date+cst_no  value:主题信息和交易信息
            Map<String, String> result_map = new HashMap<>();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            //第一次不加1
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
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
            System.out.println(bDate+"  "+eDate);
            ((BoolQueryBuilder) query).filter(queryBuilder1);

            //按照账户分桶
//            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no").size(50)
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("cst_no")
                    .subAggregation(AggregationBuilders.count("total_bank_name_count").field("part_bank_name"))
                    .subAggregation(AggregationBuilders.sum("sum_org_amt").field("org_amt")); //若该桶交易金额小于500000，则没必要再遍历了

            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(50000));
            searchSourceBuilder.aggregation(agg_self_acc_no);
            searchSourceBuilder.size(0);
            searchSourceBuilder.query(query);
            searchRequest.source(searchSourceBuilder);

                        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
            builder.setHttpAsyncResponseConsumerFactory(
                    new HttpAsyncResponseConsumerFactory
                            //修改为5000MB
                            .HeapBufferedResponseConsumerFactory(5000 * 1024 * 1024));
            RequestOptions requestOptions=builder.build();

            //参数1：搜索的请求对象，   参数2：请求配置对象   返回值：查询结果对象
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, requestOptions);
    //            System.out.println("查询条件：" + searchSourceBuilder.toString());
//            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    //            System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);

            Aggregations aggregations = searchResponse.getAggregations();

            ParsedTerms txn_per_day = aggregations.get("agg_self_acc_no");
            // 获取到分组后的所有bucket
            List<? extends Terms.Bucket> buckets = txn_per_day.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                // 解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();
                ParsedTopHits topHits = bucketAggregations.get("topHits");
                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
//                ValueCount  total_transaction = bucketAggregations.get("total_bank_name_count");
                Sum sum_org_amt = bucketAggregations.get("sum_org_amt");
                //若该桶交易金额小于500000，则没必要再遍历该桶了
                if(sum_org_amt.getValue()<500000){
                    continue;
                }
//                int total_transaction_count = (int) total_transaction.value();
//                int len = total_transaction_count;
                int len = topHits.getHits().getHits().length;
//                System.out.println(len);
                int youchu_count =0; //邮储银行交易次数
                double youchu_money = 0;//邮储银行交易金额
                int nongye_count = 0;//农业银行交易次数
                double nongye_money = 0;//农业银行交易金额
                int xinyongshe_count = 0;//信用社银行交易次数
                double xinyongshe_money = 0;//信用社银行交易金额
                String r_date = (String) sourceAsMap.get("date2");
//                客户号
                String r_cst_no = (String) sourceAsMap.get("cst_no");
//                客户名称
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
                //收款总金额
                double lend1_amt = 0;
                //收款交易笔数
                int lend1_count =0 ;
                //付款总金额
                double lend2_amt = 0;
                //付款交易笔数
                int lend2_count = 0;
                for (int j = 0; j < len; j++) {
                    Map<String, Object> sourceAsMap1 = topHits.getHits().getHits()[j].getSourceAsMap();
                    String bank_name = (String) sourceAsMap1.get("part_bank_name");
                    Double transaction_money = (Double) sourceAsMap1.get("org_amt");
                    if(bank_name.contains("邮")){
                        youchu_count += 1;
                        youchu_money += transaction_money;
                    }else if(bank_name.contains("农业")){
                        nongye_count += 1;
                        nongye_money += transaction_money;
                    }else if(bank_name.contains("信用社")){
                        xinyongshe_count += 1;
                        xinyongshe_money += transaction_money;
                    }
                    String lend_flag = (String) sourceAsMap1.get("lend_flag");
                    if(lend_flag.equals("10")){ //收
//                        lend1_amt = lend1_amt + (Double.parseDouble(sourceAsMap1.get("org_amt").toString())) ;
                        lend1_amt = lend1_amt + (Double)sourceAsMap1.get("org_amt") ;
                        lend1_count++;
                    }else if(lend_flag.equals("11")){//付
//                        lend2_amt = lend2_amt + (Double.parseDouble(sourceAsMap1.get("org_amt").toString())) ;
                        lend2_amt = lend2_amt + (Double)sourceAsMap1.get("org_amt") ;
                        lend2_count++;
                    }
                }
                if(youchu_count + nongye_count + xinyongshe_count >= len *0.5 && youchu_money + nongye_money +xinyongshe_money >= 500000){
                    //预警日期取date+1天
                    calendar3.setTime(sdf.parse(eDate));
                    calendar3.add(calendar3.DATE, 1); //预警日期：为筛出数据里最大交易日期+1天
                    r_date = sdf.format(calendar3.getTime());
                    String record = "JRSJ-008,"+r_date+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);
                    list.add(record);
                }
            }
        }
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        System.out.println("rule_8 : end");
    }
    /**
     * 计算周期：三日（交易日期）
     * 通过表tb_cred_txn、tb_acc_txn、tb_cst_unit中
     * tb_cred_txn表字段：
     * Lend_flag=11：资金收付标识:付
     * Pos_owner：信用卡消费商户名称，不能为空
     * tb_acc_txn表字段：
     * Self_acc_name账户名称=Pos_owner：信用卡消费商户名称
     * Lend_flag=11：资金收付标识：付
     * Part_acc_name：交易对方户名=表tb_cst_unit的Rep_name：法人姓名
     * 计算三日此类交易的主体数量
     * 进行条件过滤
     */
    @GetMapping("rule_9")
    @Async
    public void rule_9() throws IOException, ParseException{
        try {
            List<String> list = new ArrayList<>();
            String[] min_max = get_Min_Max("tb_acc_txn", "date2",null);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

            Calendar calendar = new GregorianCalendar();
            calendar.setTime(sdf.parse(min_max[0]));
            Calendar calendar2 = new GregorianCalendar();
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://202.118.11.39:3306/ccf41_cp?characterEncoding=UTF-8";
            Connection conn = DriverManager.getConnection(url,"soe","soe");
            Statement smt = conn.createStatement();
            for (int i=0;i<daysBetween;i++) {
                //构建boolQuery
                calendar.add(calendar.DATE, 1);
                //当前时间
                String curDay = sdf.format(calendar.getTime());
                //窗口起始时间
                String bDate = sdf.format(calendar.getTime());
                calendar2.setTime(sdf.parse(curDay));
                //窗口截至时间
                calendar2.add(calendar2.DATE, 2);
                String eDate = sdf.format(calendar2.getTime());
                //在时间段内按客户号进行分桶（基于tb_acc_txn表）
                String  cst_no_query = "select tb_acc_txn.Cst_no as tat_cst_no from tb_cred_txn, tb_acc_txn,tb_cst_unit where " +
                        "tb_cred_txn.Lend_flag = 11 and tb_cred_txn.Pos_owner is not null and " +
                        "tb_cred_txn.Pos_owner = tb_acc_txn.Self_acc_name and  tb_acc_txn.Lend_flag = 11 " +
                        "and tb_acc_txn.Part_acc_name = tb_cst_unit.Rep_name and tb_acc_txn.Date between "+"'"+bDate+"'"+" and "+"'"+eDate+"'" +
                        " Group by  tb_acc_txn.Cst_no";
                ResultSet res = smt.executeQuery(cst_no_query);
                List<String> cst_no_list = new ArrayList<>();
                while(res.next()) {
                    String cst_no = res.getString("tat_cst_no");
                    cst_no_list.add(cst_no);
                }
                res.close();
                for(int j = 0; j<cst_no_list.size();j++){
                    //按照题目描述做多表查询操作
                    String union_query = "select tb_acc_txn.Self_acc_name as tat_self_acc_name, tb_acc_txn.Date as tat_date ," +
                            "tb_acc_txn.org_amt as tat_org_amt from tb_cred_txn, tb_acc_txn,tb_cst_unit where " +
                            "tb_cred_txn.Lend_flag = 11 and tb_cred_txn.Pos_owner is not null and " +
                            "tb_cred_txn.Pos_owner = tb_acc_txn.Self_acc_name and  tb_acc_txn.Lend_flag = 11 " +
                            "and tb_acc_txn.Part_acc_name = tb_cst_unit.Rep_name and tb_acc_txn.Cst_no ="+ "'"+cst_no_list.get(j)+"' " +
                            "and tb_acc_txn.Date between "+"'"+bDate+"'"+" and "+"'"+eDate+"'";
                    ResultSet union_res = smt.executeQuery(union_query);
                    Date date_max = sdf.parse("1999-01-01");
                    String r_self_acc_name = "";
                    Calendar calendar1 = new GregorianCalendar();
                    String r_cst_no =  cst_no_list.get(j);
                    if(union_res.next() == false){
                        continue;
                    }
                    while(union_res.next()) {
                        String r_date = union_res.getString("tat_date");
                        String acc_name = union_res.getString("tat_self_acc_name");
                        if(r_self_acc_name == ""){
                            r_self_acc_name = acc_name;
                        }
                        Date date_new = sdf.parse(r_date);
                        if(date_max.compareTo(date_new)<0){
                            calendar1.setTime(date_new);
                        }
                    }
                    calendar1.add(calendar1.DATE, 1);
                    String record = "JRSJ-009,"+sdf.format(calendar1.getTime())+","+r_cst_no+","+r_self_acc_name+",,,,";
                    System.out.println(record);
                    list.add(record);
                    union_res.close();
                }
            }
            list = removeDuplicationByHashSet(list);
            CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
            System.out.println("rule_9 : end");
            //return list;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    @GetMapping("rule_10")
    @Async
    public void rule_10() throws IOException, ParseException{
        List<String> list = new ArrayList<>();
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
            //第一次不加1
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
            String curDay = sdf.format(calendar.getTime());

            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
            //过滤日期
            queryBuilder.filter(QueryBuilders.termQuery("date2", curDay));

//            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no")
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("cst_no")
                    .subAggregation(AggregationBuilders.sum("sum_org_amt").field("org_amt"));

            //按交易账户数量大于等于3过滤
            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("sum_org_amt", "sum_org_amt");
            //判断每个账户的总交易金额的结尾是否为吉利数
            Script script = new Script("(params.sum_org_amt - 666)%1000 == 0 || (params.sum_org_amt - 888)%1000 == 0 || (params.sum_org_amt - 99)%100 == 0 ");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_self_acc_no.subAggregation(bs);
            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(30000));
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
                Sum sum = bucketAggregations.get("sum_org_amt");
//                System.out.println(sum.getValueAsString());
                ParsedTopHits topHits = bucketAggregations.get("topHits");
                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                int len = topHits.getHits().getHits().length;
                String r_date = (String) sourceAsMap.get("date2");
//                客户号
                String r_cst_no = (String) sourceAsMap.get("cst_no");
//                客户名称
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
                //收款总金额
                double lend1_amt = 0;
                //收款交易笔数
                int lend1_count =0 ;
                //付款总金额
                double lend2_amt = 0;
                //付款交易笔数
                int lend2_count = 0;
                for (int j = 0; j < len; j++) {
                    Map<String, Object> sourceAsMap1 = topHits.getHits().getHits()[j].getSourceAsMap();
                    String lend_flag = (String) sourceAsMap1.get("lend_flag");
                    if(lend_flag.equals("10")){ //收
                        lend1_amt = lend1_amt + (Double.parseDouble(sourceAsMap1.get("org_amt").toString())) ;
                        lend1_count++;
                    }else if(lend_flag.equals("11")){//付
                        lend2_amt = lend2_amt + (Double.parseDouble(sourceAsMap1.get("org_amt").toString())) ;
                        lend2_count++;
                    }
                }
                String record = "JRSJ-010,"+r_date+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);
                list.add(record);
            }
        }
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        System.out.println("rule_10 : end");
//        return list;
    }

    /**
     * "计算周期：三日（交易日期）
     * 通过表tb_acc_txn中
     * 字段：
     * Agent_name:代理人姓名
     * Agent_no：代理人证件号码
     * 计算三日交易代理人为同一人的交易笔数（代理人姓名+证件号相同）
     * 计算三日总交易笔数
     * 三日交易代理人为同一人的交易笔数≥三日累计总交易笔数*60%
     * 三日交易代理人为同一人的交易金额≥500000
     * 进行条件过滤"
     */
    @GetMapping("rule_11")
    @Async
    public void rule_11() throws ParseException, IOException {
        List<String> list = new ArrayList<>();
        String[] min_max = get_Min_Max("tb_acc_txn", "date2", null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]), sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));
        Calendar calendar2 = new GregorianCalendar();

        //boolean flag = false;
        HashMap<String, String> result = new HashMap<String, String>();
        SearchRequest searchRequest = new SearchRequest("tb_acc_txn");

        for (int i = 0; i < daysBetween; i++) {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
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
            //agent_no不能为空
            QueryBuilder queryBuilder3 = QueryBuilders.termQuery("agent_no", "@N");
            ((BoolQueryBuilder) query).mustNot(queryBuilder3);
            //嵌套子聚合查询
//            TermsAggregationBuilder agg_cst_no = AggregationBuilders.terms("agg_acc_no").field("self_acc_no")
            TermsAggregationBuilder agg_cst_no = AggregationBuilders.terms("agg_acc_no").field("cst_no")
                    .subAggregation(AggregationBuilders.count("count_acc_no").field("self_acc_no"));
            agg_cst_no.subAggregation(AggregationBuilders.topHits("topHits").size(10000));
            searchSourceBuilder.aggregation(agg_cst_no);
            searchSourceBuilder.size(0);
            searchSourceBuilder.query(query);
            searchRequest.source(searchSourceBuilder);
            //System.out.println("查询条件：" + searchSourceBuilder.toString());
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            //System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);  //这里并不是topHits的数量
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms txn_per_day = aggregations.get("agg_acc_no");

            // 获取到分组后的所有bucket
            List<? extends Terms.Bucket> buckets = txn_per_day.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                // 解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();
                ParsedTopHits topHits = bucketAggregations.get("topHits");
                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                ValueCount cst_num = bucketAggregations.get("count_acc_no");
                int len = topHits.getHits().getHits().length;
                //System.out.println(len);
                String r_date = (String) sourceAsMap.get("date2");
                String r_cst_no = (String) sourceAsMap.get("cst_no");
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
                int cst_trade_num = 0;//同一个客户所有交易次数
                Map<String, Integer> agent_trade_num = new HashMap();//同一个客户同一个代理交易次数
                int agent_num = 0;
                Map<String, Double> sum_money = new HashMap();//同一个代理交易金额
                double sum = 0;
                int lend1_count = 0;//付款次数
                int lend2_count = 0;//收款次数
                double lend1_amt = 0;//付款金额
                double lend2_amt = 0;//收款金额
                cst_trade_num += cst_num.value();
                int max_trad_num = 0;
                double max_sum = 0;
                Date max_date = sdf.parse("1999-01-01");
                for (int j = 0; j < len; j++) {
                    Map<String, Object> sourceAsMap1 = topHits.getHits().getHits()[j].getSourceAsMap();
                    String M_agent_no = (String) sourceAsMap1.get("agent_no");
                    String M_agent_name = (String) sourceAsMap1.get("agent_name");
                    Double M_org_amt = (Double) sourceAsMap1.get("org_amt");
                    String M_key = M_agent_no + M_agent_name;
                    String trade_date = (String) sourceAsMap1.get("date2");
                    if (M_agent_name != "@N") {
                        if (agent_trade_num.containsKey(M_key)) {
                            agent_num = agent_trade_num.get(M_key) + 1;
                            agent_trade_num.put(M_key, agent_num);
                            if (agent_num > max_trad_num) {
                                max_trad_num = agent_num;
                            }
                        } else {
                            agent_trade_num.put(M_key, 1);
                        }
                        if (sum_money.containsKey(M_key)) {
                            sum = sum_money.get(M_key) + M_org_amt;
                            sum_money.put(M_key, sum);
                            if (sum > max_sum) {
                                max_sum = sum;
                            }
                        } else {
                            sum_money.put(M_key, M_org_amt);
                        }

                        String lend_flag = (String) sourceAsMap1.get("lend_flag");
                        if (lend_flag.equals("10")) {
                            lend1_amt += M_org_amt;
                            lend1_count++;
                        } else if (lend_flag.equals("11")) {
                            lend2_amt += M_org_amt;
                            lend2_count++;
                        }
                    }
                    Date new_date = sdf.parse(trade_date);
                    if(max_date.compareTo(new_date) < 0){
                        max_date = new_date;
                    }
                }
                if (max_trad_num >= cst_trade_num * 0.6 && max_sum >= 500000) {
                    Calendar calendar3 = new GregorianCalendar();
                    calendar3.setTime(max_date);
                    calendar3.add(calendar3.DATE,1);
                    String record = "JRSJ-011," + sdf.format(calendar3.getTime()) + "," + r_cst_no + "," + r_self_acc_name + "," + String.format("%.2f",lend1_amt) + "," +  String.format("%.2f",lend2_amt) + "," + String.valueOf(lend1_count) + "," + String.valueOf(lend2_count);
                    System.out.println(record);
                    list.add(record);
                }

            }

        }
        list = removeDuplicationByHashSet(list);
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        System.out.println("rule_11 : end");
        //return list;
    }

//    @GetMapping("/update_tb_acc_txt_Nation")
//    public void update_tb_acc_txt_Nation() throws IOException {
//        int len =22000000; //设初赛数据量
//        int unit = 10000;// 一次查询10000
//        int step = len/unit;
//
//        SearchRequest searchRequest = new SearchRequest("tb_acc_txn");//指定搜索索引
//        for(int i=0;i<step;i++){
//            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();//指定条件对象
//            QueryBuilder query = QueryBuilders.boolQuery();
//            QueryBuilder queryBuilder1 = QueryBuilders.termQuery("bord_flag","11");
////        ((BoolQueryBuilder) query).filter(queryBuilder1);
//            //Time：交易时间=00:00至06:00
//            QueryBuilder queryBuilder2 = QueryBuilders.termQuery("ip_code","@N");
//            ((BoolQueryBuilder) query).mustNot(queryBuilder2);
//
//            QueryBuilder queryBuilder3 = QueryBuilders.termQuery("ip_code","");
//            ((BoolQueryBuilder) query).mustNot(queryBuilder3);
//            sourceBuilder.fetchSource(new String[]{"ip_code"}, new String[]{});
//            //已赋值的
////            QueryBuilder queryBuilder4 = QueryBuilders.termQuery("nation","@N");
////        ((BoolQueryBuilder) query).filter(queryBuilder4);
////        sourceBuilder.query(QueryBuilders.boolQuery()
////                //查询条件  Bord_flag：跨境交易标识 11
////                .filter(QueryBuilders.termQuery("bord_flag","11")).mustNot(QueryBuilders.termQuery("ip_code","@N"))).fetchSource(new String[]{"ip_code"}, new String[]{});
////
//            sourceBuilder.query(query);
//            sourceBuilder.from(i*unit);
//            sourceBuilder.size(unit);
//            //进度
//            System.out.println(i*unit);
//            searchRequest.source(sourceBuilder);//指定查询条件
//
//            RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
//            builder.setHttpAsyncResponseConsumerFactory(
//                    new HttpAsyncResponseConsumerFactory
//                            //修改为5000MB
//                            .HeapBufferedResponseConsumerFactory(5000 * 1024 * 1024));
//            RequestOptions requestOptions=builder.build();
//
//            //参数1：搜索的请求对象，   参数2：请求配置对象   返回值：查询结果对象
////            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, requestOptions);
////            System.out.println("总条数："+searchResponse.getHits().getTotalHits().value);
//            //获取结果
//            SearchHit[] hits = searchResponse.getHits().getHits();
//            if(searchResponse.getHits().getTotalHits().value>0){
//                for(SearchHit hit:hits){
//                    String id = hit.getId();
////            System.out.println("id: "+id+"  source: "+hit.getSourceAsString());
//                    UpdateRequest updateRequest = new UpdateRequest("tb_acc_txn", id);
//
//                    Map<String, Object> kvs = new HashMap<>();
////                    System.out.println(hit.getSourceAsMap().get("ip_code"));
//                    String nation = IpUtil.getNationByIP((String) hit.getSourceAsMap().get("ip_code"));
////                    System.out.println(nation);
//                    kvs.put("nation", nation);
//                    updateRequest.doc(kvs);
////            updateRequest.timeout(TimeValue.timeValueSeconds(1));
////            updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
//                    //更新
//                    restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
//                }
//            }
//        }
//        System.out.println("更新完成");
//
//
//    }


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
    @GetMapping("rule_12")
    @Async
    public void rule_12() throws IOException, ParseException {
        System.out.println("rule_12 : begin");

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
            //第一次不加1
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
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
//            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no")
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("cst_no")
                    .subAggregation(AggregationBuilders.cardinality("count_nation").field("nation")); //跨境nation数量


            //子聚合  管道聚合不能包含子聚合
            Map<String, String> bucketsPath = new HashMap<>();
//            bucketsPath.put("agg_self_acc_no", "agg_self_acc_no");
            bucketsPath.put("count_nation", "count_nation");
            Script script = new Script("params.count_nation >= 2");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_self_acc_no.subAggregation(bs);

            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(30000));
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
//                System.out.println(len);
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
//                    String record = "日期="+r_date+", "+"客户号="+r_cst_no+", "+"客户名称="+r_self_acc_name+", nation="+r_nation+", nation数量="+count_nation.getValueAsString();
                    String record = "JRSJ-012,"+r_date+","+r_cst_no+","+r_self_acc_name+",,,,";
                    list.add(record);
//                    System.out.println(record);
                }

            }
//            restHighLevelClient.close();

        }

//        }

        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        System.out.println("rule_12 : end");
//        return list;
    }

    private List<String> holiday = new ArrayList<>();

    public  void initHoliday() {
        //holiday.add("2021-01-01");//元旦
        //holiday.add("2021-01-02");
        //holiday.add("2021-01-03");
        holiday.add("2021-02-11");//春节
        holiday.add("2021-02-12");
        holiday.add("2021-02-13");
        holiday.add("2021-02-14");
        holiday.add("2021-02-15");
        holiday.add("2021-02-16");
        holiday.add("2021-02-17");
        holiday.add("2021-02-26");//元宵节
        holiday.add("2021-04-03");//清明节
        holiday.add("2021-04-04");
        holiday.add("2021-04-05");
        //holiday.add("2021-05-01");//劳动节
        //holiday.add("2021-05-02");
        //holiday.add("2021-05-03");
        //holiday.add("2021-05-04");
        //holiday.add("2021-05-05");
        holiday.add("2021-06-12");//端午节
        holiday.add("2021-06-13");
        holiday.add("2021-06-14");
        holiday.add("2021-09-19");//中秋节
        holiday.add("2021-09-20");
        holiday.add("2021-09-21");
        //holiday.add("2021-10-01");//国庆节
        //holiday.add("2021-10-02");
        //holiday.add("2021-10-03");
        //holiday.add("2021-10-04");
        //holiday.add("2021-10-05");
        //holiday.add("2021-10-06");
        //holiday.add("2021-10-07");

    }


    /**
     * 计算周期：无（根据节假日计算）
     * 通过表tb_acc_txn中
     * 字段：
     * Org_amt：原币种交易金额
     * 计算节假日交易金额≥500000
     * 计算节假日交易笔数≥10
     * 节假日（节假日示例说明：春节-2021-02-01,01号的前一天和后一天共三天的交易金额）
     * 进行条件过滤
     */

    @GetMapping("rule_13")
    @Async
    public void rule_13() throws ParseException, IOException {
        List<String> list = new ArrayList<>();
        initHoliday();
        String[] min_max = get_Min_Max("tb_acc_txn", "date2", null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]), sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));
        Calendar calendar2 = new GregorianCalendar();
        Calendar calendar3 = new GregorianCalendar();
        HashMap<String, String> result = new HashMap<String, String>();
        SearchRequest searchRequest = new SearchRequest("tb_acc_txn");
        for (int i = 0; i < daysBetween; i++) {

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            calendar.add(calendar.DATE, 1);
//            当前时间
            String curDay = sdf.format(calendar.getTime());
            //判断是否为节假日
            //Boolean isHoliday = isWorkingDay(curDay);
            //System.out.println("今天");
            //System.out.println(curDay);
            boolean isHoilday = holiday.contains(curDay);
            //System.out.println(isHoilday);
            if (isHoilday == false)
                continue;

            //窗口起始时间
            calendar3.setTime(sdf.parse(curDay));
            calendar3.add(calendar3.DATE, -1);
            String bDate = sdf.format(calendar3.getTime());
            //System.out.println("昨天");
            //System.out.println(bDate);

            //窗口截至时间
            calendar2.setTime(sdf.parse(curDay));
            calendar2.add(calendar2.DATE, 1);
            String eDate = sdf.format(calendar2.getTime());
            // break;
//            窗口复原
//            calendar2.setTime(sdf.parse(bDate));
//            System.out.println(bDate+"  -  "+eDate);
//        3天为窗口
            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("date2").format("yyyy-MM-dd").gte(bDate).lte(eDate);
            ((BoolQueryBuilder) query).filter(queryBuilder1);

            //嵌套子聚合查询
//            TermsAggregationBuilder agg_cst_no = AggregationBuilders.terms("agg_acc_no").field("self_acc_no")
            TermsAggregationBuilder agg_cst_no = AggregationBuilders.terms("agg_acc_no").field("cst_no")
                    .subAggregation(AggregationBuilders.count("count_acc_no").field("self_acc_no"))
                    .subAggregation(AggregationBuilders.sum("sum_org_amt").field("org_amt"));


            //子聚合  管道聚合不能包含子聚合
            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("count_acc_no", "count_acc_no");
            bucketsPath.put("sum_org_amt", "sum_org_amt");
            Script script = new Script("params.count_acc_no >= 10 && params.sum_org_amt >= 500000");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_cst_no.subAggregation(bs);

            agg_cst_no.subAggregation(AggregationBuilders.topHits("topHits").size(30000));
            searchSourceBuilder.aggregation(agg_cst_no);
            searchSourceBuilder.size(0);


            searchSourceBuilder.query(query);
            searchRequest.source(searchSourceBuilder);
//            System.out.println("查询条件：" + searchSourceBuilder.toString());
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//            System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);  //这里并不是topHits的数量

            Aggregations aggregations = searchResponse.getAggregations();

            ParsedTerms txn_per_day = aggregations.get("agg_acc_no");
            // 获取到分组后的所有bucket
            List<? extends Terms.Bucket> buckets = txn_per_day.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                // 解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();
                ParsedTopHits topHits = bucketAggregations.get("topHits");
                int len = topHits.getHits().getHits().length;
                //System.out.println(len);
                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                String r_cst_no = (String) sourceAsMap.get("cst_no");
//                客户名称
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
                //收款总金额
                double lend1_amt = 0;
                //收款交易笔数
                int lend1_count = 0;
                //付款总金额
                double lend2_amt = 0;
                //付款交易笔数
                int lend2_count = 0;
                Date max_date = sdf.parse("1999-01-01");
                for (int j = 0; j < len; j++) {
                    Map<String, Object> sourceAsMap2 = topHits.getHits().getHits()[j].getSourceAsMap();
                    //根据Lend_flag判断 收 / 付
                    String lend_flag = (String) sourceAsMap2.get("lend_flag");
                    if (lend_flag.equals("10")) { //收
                        lend1_amt = lend1_amt + (Double) sourceAsMap2.get("org_amt");
                        lend1_count++;
                    } else if (lend_flag.equals("11")) {//付
                        lend2_amt = lend2_amt + (Double) sourceAsMap2.get("org_amt");
                        lend2_count++;
                    }
                    String trade_date = (String) sourceAsMap2.get("date2");
                    Date new_date = sdf.parse(trade_date);
                    if(max_date.compareTo(new_date) < 0 ){
                        max_date = new_date;
                    }
                }
//                Calendar calendar4 = new GregorianCalendar();
//                calendar4.setTime(max_date);
//                calendar4.add(calendar4.DATE,1);
                String record = "JRSJ-013," + sdf.format(max_date) + "," + r_cst_no + "," + r_self_acc_name + "," + String.format("%.2f", lend1_amt) + "," + String.format("%.2f", lend2_amt) + "," + String.valueOf(lend1_count) + "," + String.valueOf(lend2_count);
                System.out.println(record);
                list.add(record);
            }
        }
        list = removeDuplicationByHashSet(list);
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        //return list;
    }


    /**
     * 读取tb_cst_unit表中的客户号和注册资金
     */
//    @Test
    public Map<String,Double> get_reg_amt() throws IOException {
        //返回map, key:客户号   value：注册资金
        Map<String,Double> result_map = new HashMap<>();
        SearchRequest searchRequest = new SearchRequest("tb_cst_unit");//指定搜索索引
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();//指定条件对象
        sourceBuilder.query(QueryBuilders.matchAllQuery()).fetchSource(new String[]{"cst_no","code"}, new String[]{});//查询cst_no和code字段，code错位，代表注册资金
        sourceBuilder.size(100000);
        searchRequest.source(sourceBuilder);//指定查询条件

        //参数1：搜索的请求对象，   参数2：请求配置对象   返回值：查询结果对象
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("总条数："+searchResponse.getHits().getTotalHits().value);
        //获取结果
        SearchHit[] hits = searchResponse.getHits().getHits();
        for(SearchHit hit:hits){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String cst_no = (String) sourceAsMap.get("cst_no");
            Double reg_amt = Double.parseDouble((String) sourceAsMap.get("code"));
            result_map.put(cst_no,reg_amt);
//            System.out.println(cst_no+"  "+reg_amt);
        }
        return result_map;
    }

    /**
     * "计算周期：每日（交易日期）
     * 通过表tb_acc_txn、tb_cst_unit中
     * 字段：
     * 表tb_acc_txn：Org_amt：原币种交易金额
     * 表tb_cst_unit：Reg_amt：注册本金
     * 日累计交易金额≥注册本金
     * 日累计交易金额≥500000
     * 进行条件过滤"
     * @throws IOException
     * @throws ParseException
     */
    @GetMapping("rule_14")
    @Async
    public void rule_14() throws IOException, ParseException {
        System.out.println("rule_14 : begin");

        List<String> list = new ArrayList<>();
        //仅 公司账户
        String[] min_max = get_Min_Max("tb_acc_txn", "date2",QueryBuilders.termQuery("acc_type","12"));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));
        //获取客户对应的注册资金map
        Map<String,Double> reg_amt_map = get_reg_amt();
        for (int i=0;i<daysBetween;i++) {
            SearchRequest searchRequest = new SearchRequest("tb_acc_txn");

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        QueryBuilder query = QueryBuilders.matchQuery("Lend_flag","11");
            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            //第一次不加1
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
// 这个时间就是日期往后推一天的结果
            String curDay = sdf.format(calendar.getTime());
//            System.out.println(curDay);
//        一天为窗口
//            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("date2").format("yyyy-MM-dd").gte(curDay).lte(curDay);
            QueryBuilder queryBuilder1 = QueryBuilders.termQuery("date2",curDay);
            ((BoolQueryBuilder) query).filter(queryBuilder1);
//        //公司账户
            QueryBuilder queryBuilder2 = QueryBuilders.termQuery("acc_type", "12");
            ((BoolQueryBuilder) query).filter(queryBuilder2);



            //嵌套子聚合查询  以self_acc_name账户名称分桶
//            TermsAggregationBuilder agg_self_acc_name = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no")
            TermsAggregationBuilder agg_self_acc_name = AggregationBuilders.terms("agg_self_acc_no").field("cst_no") //按cst_no聚合
                    .subAggregation(AggregationBuilders.sum("sum_org_amt").field("org_amt"));

            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("sum_org_amt","sum_org_amt");
            //累计交易金额大于500000
            Script script = new Script("params.sum_org_amt >= 500000");
//            Script script = new Script("params.count_self_bank_code>=3");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_self_acc_name.subAggregation(bs);

            agg_self_acc_name.subAggregation(AggregationBuilders.topHits("topHits").size(30000));
            searchSourceBuilder.aggregation(agg_self_acc_name);
            searchSourceBuilder.size(0);


            searchSourceBuilder.query(query);

            searchRequest.source(searchSourceBuilder);
//            System.out.println("查询条件：" + searchSourceBuilder.toString());

            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            Aggregations aggregations = searchResponse.getAggregations();

            ParsedTerms txn_per_day = aggregations.get("agg_self_acc_no");
            // 获取到分组后的所有bucket
            List<? extends Terms.Bucket> buckets = txn_per_day.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                // 解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();

                ParsedTopHits topHits = bucketAggregations.get("topHits");

                int len = topHits.getHits().getHits().length;
//                System.out.println(len);
                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                String r_date = (String) sourceAsMap.get("date2");
//                折人民币交易金额-收
                String r_lend1 = "0";
//                折人民币交易金额-付
                Sum r_lend2 = bucketAggregations.get("sum_org_amt");
//                客户号
                String r_cst_no = (String) sourceAsMap.get("cst_no");
//                客户名称
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
                //获取该客户的注册资金
                Double reg_amt = 0.0;//初始为0
                reg_amt = reg_amt_map.get(r_cst_no);

                //总交易金额
                Sum sum_org_amt =  bucketAggregations.get("sum_org_amt");
                //若总交易金额不大于注册本金，则没必要遍历该桶
                //若该客户的注册资金不为0，则没必要遍历该桶
                if(sum_org_amt.getValue() < reg_amt || reg_amt > 0){
                    continue;
                }

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
                        lend1_amt = lend1_amt + (Double) sourceAsMap2.get("org_amt");
                        lend1_count++;
                    }else if(lend_flag.equals("11")){//付
                        lend2_amt = lend2_amt + (Double) sourceAsMap2.get("org_amt");
                        lend2_count++;
                    }

                }
                String record = "JRSJ-014,"+r_date+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);
                list.add(record);
//                System.out.println(record);
//                System.out.println("注册资金："+reg_amt);

            }
//            restHighLevelClient.close();

        }

//        }
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        System.out.println("rule_14 : end");
//        return list;
    }


    @GetMapping("rule_14_old")
    @Async
    public void rule_14_old() throws ParseException, IOException{
        try {
            List<String> list = new ArrayList<>();
            String[] min_max = get_Min_Max("tb_acc_txn", "date2",null);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

            Calendar calendar = new GregorianCalendar();
            calendar.setTime(sdf.parse(min_max[0]));
            Calendar calendar2 = new GregorianCalendar();
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://202.118.11.39:3306/ccf41_cp?characterEncoding=UTF-8";
            Connection conn = DriverManager.getConnection(url,"soe","soe");
            Statement smt = conn.createStatement();
            for (int i=0;i<daysBetween;i++) {
                calendar.add(calendar.DATE, 1);
                String curDay = sdf.format(calendar.getTime());
                //查出每个时间段内符合条件的客户号（基于tb_acc_txn表）
                String  cst_no_query = "SELECT tb_acc_txn.Cst_no as tat_cst_no, sum(tb_acc_txn.Org_amt) as total_amt from tb_acc_txn JOIN tb_cst_unit ON tb_acc_txn.Cst_no = tb_cst_unit.Cst_no " +
                        "Where tb_acc_txn.Date = '"+curDay+"'" +" GROUP BY tb_acc_txn.Cst_no";
                ResultSet res = smt.executeQuery(cst_no_query);
                List<String> cst_no_list = new ArrayList<>();
                List<Double> total_amt_list = new ArrayList<>();
                //将每日分组后的账户添加到list中
                while(res.next()) {
                    String acc_no = res.getString("tat_cst_no");
                    Double total_amt = res.getDouble("total_amt");
                    cst_no_list.add(acc_no);
                    total_amt_list.add(total_amt);
                }
                res.close();
                //从list中取出每个账户，并按条件查询每日该账户的记录
                for(int j = 0; j<cst_no_list.size();j++){
                    //按照题目描述做多表查询操作
                    String union_query = "SELECT tb_acc_txn.Lend_flag as tat_lend_flag, tb_cst_unit.Code as tcu_reg_amt,tb_acc_txn.org_amt as tat_org_amt, tb_acc_txn.Cst_no as tat_cst_no, tb_acc_txn.Date as tat_date," +
                            "tb_acc_txn.Self_acc_name as tat_self_acc_name from tb_acc_txn JOIN tb_cst_unit ON tb_acc_txn.Cst_no = tb_cst_unit.Cst_no" +
                            " where tb_acc_txn.Date = '"+curDay+"'"+
                            "and tb_acc_txn.Cst_no = "+"'"+cst_no_list.get(j)+"'";
                    ResultSet union_res = smt.executeQuery(union_query);
                    Date date_max = sdf.parse("1999-01-01");
                    String r_self_acc_name = "";
                    Calendar calendar1 = new GregorianCalendar();
                    String r_cst_no = cst_no_list.get(j);
                    Double total_amt_per_day = total_amt_list.get(j);
                    //收款总金额
                    double lend1_amt = 0;
                    //收款交易笔数
                    int lend1_count =0 ;
                    //付款总金额
                    double lend2_amt = 0;
                    //付款交易笔数
                    int lend2_count = 0;
                    double reg_amt = 0;
                    while(union_res.next()) {
                        Double reg_amt1 = union_res.getDouble("tcu_reg_amt");
                        String r_date = union_res.getString("tat_date");
                        String cst_no = union_res.getString("tat_cst_no");
                        String acc_name = union_res.getString("tat_self_acc_name");
                        String lend_flag = union_res.getString("tat_lend_flag");
                        Double lend_amt = union_res.getDouble("tat_org_amt");
                        if(reg_amt == 0){
                            reg_amt = reg_amt1;
                        }
                        if(lend_flag.equals("10")){
                            lend1_count += 1;
                            lend1_amt += lend_amt;
                        }
                        else if(lend_flag.equals("11")){
                            lend2_count += 1;
                            lend2_amt += lend_amt;
                        }
                        if(r_cst_no == ""){
                            r_cst_no = cst_no;
                        }
                        if(r_self_acc_name == ""){
                            r_self_acc_name = acc_name;
                        }
                        Date date_new = sdf.parse(r_date);
                        if(date_max.compareTo(date_new)<0){
                            calendar1.setTime(date_new);
                        }
                    }
                    if(total_amt_per_day >= reg_amt  && total_amt_per_day >= 500000){
                        calendar1.add(calendar1.DATE, 1);
                        String record = "JRSJ-014,"+sdf.format(calendar1.getTime())+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);
                        System.out.println(record);
                        list.add(record);
                    }
                    union_res.close();
                }
            }



            // 关闭流 (先开后关)
            smt.close();
            conn.close();
            list = removeDuplicationByHashSet(list);
            CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
            System.out.println("rule_14 : end");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
    /**
     * 计算周期：无（交易日期）
     * 通过表tb_cst_unit中
     * 字段：
     * Id_type2：法人证件类型
     * Id_no2：法人证件号码
     * 截取Id_type2法人证件类型=110001、110003的Id_no2法人证件号码的出生日期计算年龄<20岁的主体
     * 计算该主体交易金额>1元
     * 进行条件过滤
     */
    @GetMapping("rule_15")
    @Async
    public void rule_15() throws ParseException, IOException {
        try{
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://202.118.11.39:3306/ccf41_cp?characterEncoding=UTF-8";
            Connection conn = DriverManager.getConnection(url,"soe","soe");
            List<String> list = new ArrayList<>();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Statement smt = conn.createStatement();
            String  cst_no_query = "SELECT tb_acc_txn.Cst_no as tat_cst_no from tb_acc_txn JOIN tb_cst_pers " +
                    " ON tb_cst_pers.Cst_no=tb_acc_txn.Cst_no " +
                    "where  tb_cst_pers.Id_type = '110021' " +
                    "and (tb_cst_pers.Address1 = tb_cst_pers.Address2 or tb_cst_pers.Address1 = tb_cst_pers.Address3 or tb_cst_pers.Address2 = tb_cst_pers.Address3) " +
                    "and tb_acc_txn.Org_amt<10 GROUP BY tb_acc_txn.Cst_no";
            ResultSet res = smt.executeQuery(cst_no_query);
            List<String> cst_no_list = new ArrayList<>();
            while(res.next()) {
                String acc_no = res.getString("tat_cst_no");
                cst_no_list.add(acc_no);
            }
            res.close();
            for(int j = 0; j<cst_no_list.size();j++) {
                //按照题目描述做多表查询操作
                String union_query = "SELECT tb_acc_txn.org_amt as tat_org_amt, tb_acc_txn.Cst_no as tat_cst_no, tb_acc_txn.Self_acc_name as tat_self_acc_name, " +
                        "tb_acc_txn.Date as tat_date, tb_acc_txn.Lend_flag as tat_lend_flag from tb_acc_txn JOIN tb_cst_pers " +
                        " ON tb_cst_pers.Cst_no=tb_acc_txn.Cst_no " +
                        "where  tb_cst_pers.Id_type = '110021' " +
                        "and (tb_cst_pers.Address1 = tb_cst_pers.Address2 or tb_cst_pers.Address1 = tb_cst_pers.Address3 or tb_cst_pers.Address2 = tb_cst_pers.Address3) " +
                        "and tb_acc_txn.Org_amt<10 and tb_acc_txn.Cst_no = "+ cst_no_list.get(j);
                ResultSet union_res = smt.executeQuery(union_query);
                Date date_max = sdf.parse("1999-01-01");
                String r_self_acc_name = "";
                Calendar calendar1 = new GregorianCalendar();
                String r_cst_no = "";
                boolean out_flag = false;
                double lend1_amt = 0;
                //收款交易笔数
                int lend1_count =0 ;
                //付款总金额
                double lend2_amt = 0;
                //付款交易笔数
                int lend2_count = 0;
                while(union_res.next()) {
                    if (out_flag == false) {
                        out_flag = true;
                    }
                    String r_date = union_res.getString("tat_date");
                    String cst_no = union_res.getString("tat_cst_no");
                    String acc_name = union_res.getString("tat_self_acc_name");
                    String lend_flag = union_res.getString("tat_lend_flag");
                    Double lend_amt = union_res.getDouble("tat_org_amt");
                    if(lend_flag.equals("10")){
                        lend1_count += 1;
                        lend1_amt += lend_amt;
                    }
                    if(lend_flag.equals("11")){
                        lend2_count += 1;
                        lend2_amt += lend_amt;
                    }
                    if(r_cst_no == ""){
                        r_cst_no = cst_no;
                    }
                    if(r_self_acc_name == ""){
                        r_self_acc_name = acc_name;
                    }
                    Date date_new = sdf.parse(r_date);
                    if(date_max.compareTo(date_new)<0){
                        calendar1.setTime(date_new);
                    }

                }
                if(out_flag == true){
                    calendar1.add(calendar1.DATE, 1);
                    String record = "JRSJ-015,"+sdf.format(calendar1.getTime())+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);
//                    System.out.println(record);
                    list.add(record);
                }
                union_res.close();
            }
            // 关闭流 (先开后关)
            smt.close();
            conn.close();
            list = removeDuplicationByHashSet(list);
            CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
            System.out.println("rule_15 : end");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
     * @return
     */
    @GetMapping("rule_16")
    @Async
    public void rule_16() throws ParseException, IOException {
        System.out.println("rule_16 : begin");
        List<String> list = new ArrayList<>();

        //获取最大和最小日期范围
        String[] min_max = get_Min_Max("tb_acc_txn","date2",null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));

        Calendar calendar2 = new GregorianCalendar();

        SearchRequest searchRequest = new SearchRequest("tb_acc_txn");
        boolean flag = false;
//        3天的窗口可能含有重复结果
//        key:代理人身份证号    value:预警日期
        HashMap<String, String> result = new HashMap<String, String>();
        for(int i = 0; i < daysBetween; i++){

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            //第一次不加1
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
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
            //交易金额>=10000
            QueryBuilder queryBuilder2 = QueryBuilders.rangeQuery("org_amt").gte(10000);
            ((BoolQueryBuilder) query).filter(queryBuilder2);

            //按账户分桶
//            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no");
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("cst_no");

//            Map<String, String> bucketsPath = new HashMap<>();
//            bucketsPath.put("sum_org_amt","sum_org_amt");
//            bucketsPath.put("count","_count");
//            //三日交易金额≥10000 且 交易金额是10000的整数倍的整万
//            Script script = new Script("params.sum_org_amt >= 10000 && params.sum_org_amt % 10000 == 0");
//            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
//            agg_self_acc_no.subAggregation(bs);
            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(30000));
            sourceBuilder.query(query);
            sourceBuilder.aggregation(agg_self_acc_no);
            sourceBuilder.size(0);

            searchRequest.source(sourceBuilder);
//            System.out.println("查询条件："+sourceBuilder.toString());
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//            System.out.println("总条数："+searchResponse.getHits().getTotalHits().value);

            //处理聚合结果
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms agg_self_acc_no_terms = aggregations.get("agg_self_acc_no");
            //获取分组后的所有bucket
            List<? extends Terms.Bucket> buckets = agg_self_acc_no_terms.getBuckets();

            for (Terms.Bucket bucket : buckets) {
                // 解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();

                ParsedTopHits topHits = bucketAggregations.get("topHits");

                int len = topHits.getHits().getHits().length;
//                System.out.println(len);
                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                String r_date = (String) sourceAsMap.get("date2");
//                String r_nation = (String) sourceAsMap.get("nation");
                String r_cst_no = (String) sourceAsMap.get("cst_no");
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
//                ParsedCardinality count_nation =  bucketAggregations.get("count_nation");
//                String record = "日期="+r_date+", "+"客户号="+r_cst_no+", "+"客户名称="+r_self_acc_name+", nation="+r_nation+", nation数量="+count_nation.getValueAsString();
//                System.out.println(record);


                //收款总金额
                double lend1_amt = 0;
                //收款交易笔数
                int lend1_count =0 ;
                //付款总金额
                double lend2_amt = 0;
                //付款交易笔数
                int lend2_count = 0;
                //统计交易对手各自重复次数
                HashMap<String, Integer> part_count_map = new HashMap<String, Integer>();
                //标识该主体的相关交易对手重复次数是否大于3
                boolean part_count_rep = false;
                //统计该桶内的收/付款总金额以及 收/付 次数，统计交易对手账户出现次数
                for (int j = 0; j < len; j++) {
                    Map<String, Object> sourceAsMap2 = topHits.getHits().getHits()[j].getSourceAsMap();
                    //单笔交易金额是10000的整数倍(前提条件是整数)
                    double org_amt = (Double) sourceAsMap2.get("org_amt");
                    if((org_amt % (int)org_amt == 0) && ((int) org_amt % 10000 ==0 )){
                        r_date = (String) sourceAsMap.get("date2");
                        //根据Lend_flag判断 收 / 付
                        String lend_flag = (String) sourceAsMap2.get("lend_flag");
                        String part_acc_no = (String) sourceAsMap2.get("part_acc_no");
                        if(lend_flag.equals("10")){ //收
                            lend1_amt = lend1_amt + (Double) sourceAsMap2.get("org_amt");
                            lend1_count++;
                        }else if(lend_flag.equals("11")){//付
//                            lend2_amt = lend2_amt + (Double) sourceAsMap2.get("org_amt");
                            lend2_amt = lend2_amt + org_amt;
                            lend2_count++;
                        }
                        if(part_count_map.containsKey(part_acc_no)){
                            //如果该对手账户已存在，交易次数就+1
                            int part_acc_count = part_count_map.get(part_acc_no);
                            part_acc_count ++;
                            part_count_map.put(part_acc_no,part_acc_count);
                            if(part_acc_count>=3){
                                part_count_rep = true;
                            }
                        }else {
                            //若该交易对手还不存在，就存入，且交易次数=1
                            part_count_map.put(part_acc_no,1);
                        }
                    }


                }
                //该主体的交易对手重复次数>=3
                if(part_count_rep){
//                    boolean isNew = true;
//                    if(result.containsKey(r_cst_no)){
//                        String exist_date = result.get(r_cst_no);
//                        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
//                        if(daysBetween(sdf.parse(r_date),sdf.parse(exist_date))>=3){
////                        更新value
//                            result.put(r_cst_no,r_date);
//                        }else {
//                            isNew = false;
//                        }
//                    }else {
//                        result.put(r_cst_no,r_date);
//                    }
//                    if(isNew)
//                    {
//                    String record = "日期="+r_date+", "+"客户号="+r_cst_no+", "+"客户名称="+r_self_acc_name+", nation="+r_nation+", nation数量="+count_nation.getValueAsString();
                    Calendar calendar3 = new GregorianCalendar();
                    calendar3.setTime(sdf.parse(r_date));
                    calendar3.add(calendar3.DATE, 1); //预警日期：为筛出数据里最大交易日期+1天
                    String record = "JRSJ-016,"+sdf.format(calendar3.getTime())+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);

                    list.add(record);
//                        System.out.println(record);
//                    }
                }



            }


        }
        list = removeDuplicationByHashSet(list);
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        System.out.println("rule_16 : end");
//        return list;
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
    @GetMapping("rule_17")
    @Async
    public void rule_17() throws ParseException, IOException {
        List<String> list = new ArrayList<>();
        //获取最大和最小日期范围
        String[] min_max = get_Min_Max("tb_acc","open_time",QueryBuilders.termQuery("open_type1","11"));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));

        Calendar calendar2 = new GregorianCalendar();

        SearchRequest searchRequest = new SearchRequest("tb_acc");
        boolean flag = false;
//        3天的窗口可能含有重复结果
//        key:代理人身份证号    value:预警日期
        HashMap<String, String> result = new HashMap<String, String>();

        for(int i = 0; i < daysBetween; i++){

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            //第一次不加1
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
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
            //只取具有代理关系的
            QueryBuilder queryBuilder2 = QueryBuilders.termQuery("open_type1","11");
            ((BoolQueryBuilder) query).filter(queryBuilder2);
            //agent_no不为空
            QueryBuilder queryBuilder3 = QueryBuilders.termQuery("agent_no","@N");
            ((BoolQueryBuilder) query).mustNot(queryBuilder3);

            //按账号分桶
//            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no");
            //按self_acc_name分桶
//            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_name");
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("cst_no");

            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(10000));
            sourceBuilder.query(query);
            sourceBuilder.aggregation(agg_self_acc_no);
            sourceBuilder.size(0);

            searchRequest.source(sourceBuilder);
            //System.out.println("查询条件："+sourceBuilder.toString());
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            //System.out.println("总条数："+searchResponse.getHits().getTotalHits().value);

            //处理聚合结果
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedStringTerms parsedStringTerms = aggregations.get("agg_self_acc_no");
            //获取分组后的所有bucket
            List<? extends Terms.Bucket> buckets = parsedStringTerms.getBuckets();
            Calendar calendar3 = new GregorianCalendar();
            for (Terms.Bucket bucket : buckets) {
                //解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();
                ParsedTopHits topHits = bucketAggregations.get("topHits");
                int len = topHits.getHits().getHits().length;
                //System.out.println("有"+len+"个文档");
                Map<String ,Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                String r_date = (String) sourceAsMap.get("open_time");//开户日期
                String r_cst_no = (String) sourceAsMap.get("cst_no");//客户号
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");//账户户名

                //存放agent_no的集合
                Set<String> agent_set  = new HashSet<>();
                //标识该主体是否符合筛选条件
                boolean agent_no_rep = false;
                Map<String, Object> sourceAsMap2 = topHits.getHits().getHits()[0].getSourceAsMap();
                String agent_no = (String) sourceAsMap2.get("agent_no");
//                if(agent_no.equals("@N")){
//                    continue;
//                }
                agent_set.add(agent_no);
                if(len==1){
                    //只有一条代理信息时
                    agent_no_rep = false;
                }else {
                    String max_date = "";
                    for(int j = 0;j < len;j++ ){
                        Map<String, Object> sourceAsMap3 = topHits.getHits().getHits()[j].getSourceAsMap();
                        String agent_no1 = (String) sourceAsMap3.get("agent_no");
                        String open_time = (String) sourceAsMap3.get("open_time"); //开户日期
                        String birthday = ((String) sourceAsMap3.get("id_no")).substring(6,14);  //出生日期
                        int age = age(open_time,birthday);

                        if(age < 18 || age > 70){
                            if(agent_set.contains(agent_no1)){
                                //相同代理人
                                max_date = open_time;
                                agent_no_rep = true;
                            }
                            agent_set.add(agent_no1);
                        }
//                        if(!(agent_set.contains(agent_no1))){
//                            //有多个agent_no,不符合条件
//                            agent_no_rep = false;
//                            break;
//                        }else {
//                            if(age>=18 && age<=70){
//                                //年龄正常，不符合条件
//                                agent_no_rep = false;
//                                break;
//                            }
//                        }
                    }
                    if(agent_no_rep){
                        calendar3.setTime(sdf.parse(max_date));
                        calendar3.add(calendar3.DATE, 1); //预警日期：为筛出数据里最大交易日期+1天
                        String record1 = "JRSJ-017,"+sdf2.format(calendar3.getTime())+","+r_cst_no+","+r_self_acc_name+",,,,";

                        list.add(record1);
//                        System.out.println(record1);
                    }
                }
//                if(agent_no_rep){
//                    boolean isNew = true;
//                    if(result.containsKey(r_cst_no)){
//                        String exist_date = result.get(r_cst_no);
////                        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
//                        if(daysBetween(sdf.parse(r_date),sdf.parse(exist_date))>=3){
////                        更新value
//                            result.put(r_cst_no,r_date);
//                        }else {
//                            isNew = false;
//                        }
//                    }else {
//                        result.put(r_cst_no,r_date);
//                    }
//                    if(isNew)
//                    {
//                        String record1 = "JRSJ-017,"+r_date+","+r_cst_no+","+r_self_acc_name;
//
////                    list.add(record);
//                        System.out.println(record1);
//                    }
//                }

            }
        }
        list = removeDuplicationByHashSet(list);
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        System.out.println("rule_17 : end");
    }


    /**
     * 计算周期：每日（交易日期）
     * 通过表tb_acc_txn中
     * 字段：
     * Time：交易时间
     * Org_amt：原币种交易金额
     * 日累计交易时间在21:00至次日07:00时间段内的交易笔数≥10笔
     * 计算在该时间内单笔金额≥1000元
     * 进行条件过滤
     */
    @GetMapping("rule_18")
    @Async
    public void rule_18() throws ParseException, IOException {
        List<String> list = new ArrayList<>();
        //获取最大和最小日期范围
        String[] min_max = get_Min_Max("tb_acc_txn","date2",QueryBuilders.termQuery("acc_type","12"));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));
        Calendar calendar = new GregorianCalendar();
        Calendar calendar2 = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));
        SearchRequest searchRequest = new SearchRequest("tb_acc_txn");
        //        key:代理人身份证号    value:预警日期
        HashMap<String, String> result = new HashMap<String, String>();
        for(int i = 0;i < daysBetween; i++) {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            //第一次不加1
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
            //当前时间
            String curDay = sdf.format(calendar.getTime());
            //窗口起始时间
            String bDate = sdf.format(calendar.getTime());
            calendar2.setTime(sdf.parse(curDay));
            //窗口截止时间
            calendar2.add(calendar2.DATE,1);
            String eDate = sdf.format(calendar2.getTime());
            //2天为窗口
            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("date2").format("yyyy-MM-dd").gte(bDate).lte(eDate);
            System.out.println(bDate+"  "+eDate);
            ((BoolQueryBuilder) query).filter(queryBuilder1);
            //单位账户，非个人账户
            QueryBuilder queryBuilder4 = QueryBuilders.termQuery("acc_type","12");
            ((BoolQueryBuilder) query).filter(queryBuilder4);
            //1.交易时间在21:00至次日07:00时间段内
            ((BoolQueryBuilder) query).must(QueryBuilders.boolQuery().should(QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("date2",bDate)).must(QueryBuilders.rangeQuery("time").gte("210000").lte("000000")))
                    .should(QueryBuilders.boolQuery()
                            .must(QueryBuilders.termQuery("date2",eDate)).must(QueryBuilders.rangeQuery("time").gte("000000").lt("070000"))));
//            QueryBuilder queryBuilder2 = QueryBuilders.rangeQuery("time").gt("070000").lt("210000");
//            ((BoolQueryBuilder) query).mustNot(queryBuilder2);
            //2.单笔金额≥1000元
            QueryBuilder queryBuilder3 = QueryBuilders.rangeQuery("org_amt").gte(1000);
            ((BoolQueryBuilder) query).filter(queryBuilder3);

            //按照账户分桶
//            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no").size(10000);
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("cst_no").size(10000);
            Map<String, String> bucketsPath = new HashMap<>();
            bucketsPath.put("_count","_count");
            //日累计交易时间在21:00至次日07:00时间段内的交易笔数≥10笔
            Script script = new Script("params._count >= 10");
            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
            agg_self_acc_no.subAggregation(bs);
            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(100000));
            sourceBuilder.query(query);
            sourceBuilder.aggregation(agg_self_acc_no);
            sourceBuilder.size(0);

            searchRequest.source(sourceBuilder);
            //System.out.println("查询条件：" + sourceBuilder.toString());
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            //System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);

            //处理聚合结果
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms agg_self_acc_no_terms = aggregations.get("agg_self_acc_no");

            //获取分组后的所有bucket
            List<? extends Terms.Bucket> buckets = agg_self_acc_no_terms.getBuckets();
            Calendar calendar3 = new GregorianCalendar();
            for (Terms.Bucket bucket : buckets) {
                //解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();

                ParsedTopHits topHits = bucketAggregations.get("topHits");

                int len = topHits.getHits().getHits().length;
                //System.out.println(len);//一个桶里有多少文档
                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                String r_date = (String) sourceAsMap.get("date2");//交易日期
                String r_cst_no = (String) sourceAsMap.get("cst_no");//客户号
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");//账号

                //收款总金额
                double lend1_amt = 0;
                //收款交易笔数
                int lend1_count = 0;
                //付款总金额
                double lend2_amt = 0;
                //付款交易笔数
                int lend2_count = 0;
                //交易总笔数
                int lend_count = 0;
                //标识该主体交易笔数是否≥10笔
                boolean lend_count_rep = false;

                String max_date = "";

                for (int j = 0; j < len; j++) {
                    Map<String, Object> sourceAsMap1 = topHits.getHits().getHits()[j].getSourceAsMap();
                    r_date = (String) sourceAsMap1.get("date2");
                    double org_amt = (Double) sourceAsMap1.get("org_amt"); //折人民币交易金额
                    String lend_flag = (String) sourceAsMap1.get("lend_flag"); //资金收付标识
                    if (lend_flag.equals("10")) {//收
                        lend1_amt = lend1_amt + (Double) sourceAsMap1.get("org_amt");
                        lend1_count++;
                    } else if (lend_flag.equals("11")) {//付
                        lend2_amt = lend2_amt + org_amt;
                        lend2_count++;
                    }
                    lend_count = lend1_count + lend2_count;
//                    if(lend_count >= 10){
//                        max_date = date2;
//                        lend_count_rep = true;
//                    }

                }
//                if(lend_count_rep){
                calendar3.setTime(sdf.parse(r_date));
                calendar3.add(calendar3.DATE, 1); //预警日期：为筛出数据里最大交易日期+1天
                String record1 = "JRSJ-018,"+sdf2.format(calendar3.getTime())+","+r_cst_no+","+r_self_acc_name+ "," + String.format("%.2f", lend1_amt) + "," + String.format("%.2f", lend2_amt) + "," + String.valueOf(lend1_count) + "," + String.valueOf(lend2_count);
                list.add(record1);
//                System.out.println(record1);
//                }

            }

        }
        removeDuplicationByHashSet(list);
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);

    }

    /**
     * 计算周期：无（交易日期）
     * 通过表tb_cst_unit中
     * 字段：
     * Id_type2：法人证件类型
     * Id_no2：法人证件号码
     * 截取Id_type2法人证件类型=110001、110003的Id_no2法人证件号码的出生日期计算年龄<20岁的主体
     * 计算该主体交易金额>1元
     * 进行条件过滤
     */
    @GetMapping("rule_19")
    @Async
    public void rule_19(){
        List<String> list = new ArrayList<>();
        try{
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://202.118.11.39:3306/ccf41_cp?characterEncoding=UTF-8";
            Connection conn = DriverManager.getConnection(url,"soe","soe");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Statement smt = conn.createStatement();
            //查出符合条件的客户号
            //1.id_type2=110001或id_type2=110003
            //2.org_amt>1
            String cst_no_query = "SELECT tb_cst_unit.Cst_no as tbu_cst_no from tb_cst_unit JOIN tb_acc_txn ON tb_acc_txn.Cst_no = tb_cst_unit.Cst_no "+
                    "where (tb_cst_unit.Id_type2 = '110001' or tb_cst_unit.Id_type2 = '110003') "+
                    "and tb_acc_txn.Org_amt > 1 "+
                    "GROUP BY tb_acc_txn.Cst_no";
            ResultSet res = smt.executeQuery(cst_no_query);
            List<String> cst_no_list = new ArrayList<>();
            while(res.next()) {
                String acc_no = res.getString("tbu_cst_no");
                cst_no_list.add(acc_no);
            }
            res.close();
            //从list中去除每个账户，并按条件查询该账户的记录
            for(int j = 0; j<cst_no_list.size();j++){
                //按照题目描述做多表查询操作
                String union_query = "SELECT tb_cst_unit.Id_no2 as tat_id_no,tb_acc_txn.Lend_flag as tat_lend_flag, tb_acc_txn.org_amt as tat_org_amt, tb_acc_txn.Cst_no as tat_cst_no, tb_acc_txn.Date as tat_date," +
                        "tb_acc_txn.Self_acc_name as tat_self_acc_name from tb_acc_txn JOIN tb_cst_unit ON tb_acc_txn.Cst_no = tb_cst_unit.Cst_no "+
                        "where (tb_cst_unit.Id_type2 = '110001' or tb_cst_unit.Id_type2 = '110003') "+
                        "and tb_acc_txn.Org_amt > 1 "+
                        "and tb_acc_txn.Cst_no = "+ cst_no_list.get(j);
                ResultSet union_res = smt.executeQuery(union_query);
                Date date_max = sdf.parse("1999-01-01");
                String r_self_acc_name = "";
                Calendar calendar1 = new GregorianCalendar();
                String r_cst_no = cst_no_list.get(j);
                boolean out_flag = false;
                boolean age_flag = false;
                double lend1_amt = 0;
                //收款交易笔数
                int lend1_count =0 ;
                //付款总金额
                double lend2_amt = 0;
                //付款交易笔数
                int lend2_count = 0;
                while(union_res.next()) {
                    if(out_flag == false){
                        out_flag = true;
                    }
                    String r_date = union_res.getString("tat_date");
                    String cst_no = union_res.getString("tat_cst_no");
                    String acc_name = union_res.getString("tat_self_acc_name");
                    String lend_flag = union_res.getString("tat_lend_flag");
                    Double lend_amt = union_res.getDouble("tat_org_amt");
                    String id_no = union_res.getString("tat_id_no");
                    int age = age(r_date,id_no.substring(6,14));
                    if(age<20){
                        age_flag = true;
                        if(lend_flag.equals("10")){
                            lend1_count += 1;
                            lend1_amt += lend_amt;
                        }
                        if(lend_flag.equals("11")){
                            lend2_count += 1;
                            lend2_amt += lend_amt;
                        }
                        if(r_cst_no == ""){
                            r_cst_no = cst_no;
                        }
                        if(r_self_acc_name == ""){
                            r_self_acc_name = acc_name;
                        }
                        Date date_new = sdf.parse(r_date);
                        if(date_max.compareTo(date_new)<0){
                            calendar1.setTime(date_new);
                        }
                    }

                }
                if(out_flag == true && age_flag == true){
                    calendar1.add(calendar1.DATE,0);
                    //日期为交易日期
                    String record = "JRSJ-019,"+sdf.format(calendar1.getTime())+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);
                    System.out.println(record);
                    list.add(record);
                }
                union_res.close();
            }
            smt.close();
            conn.close();

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
    }



    /**
     * 计算周期：三日（交易日期）
     * 通过表tb_acc_txn中
     * 字段：
     * Nation：交易对方国家地区
     * 计算三日Nation重复≥5的主体
     * 进行条件过滤
     */
    @GetMapping("rule_20")
    @Async
    public void rule_20() throws IOException, ParseException {
        List<String> list = new ArrayList<>();
        //获取最大和最小日期范围
        String[] min_max = get_Min_Max("tb_acc_txn","date2",QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("bord_flag","11")));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));

        Calendar calendar = new GregorianCalendar();
        calendar.setTime(sdf.parse(min_max[0]));

        Calendar calendar2 = new GregorianCalendar();

        SearchRequest searchRequest = new SearchRequest("tb_acc_txn");

        boolean flag = false;
//        3天的窗口可能含有重复结果
//        key:代理人身份证号    value:预警日期
        HashMap<String, String> result = new HashMap<String, String>();

        for(int i = 0; i < daysBetween; i++){

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            //构建boolQuery
            QueryBuilder query = QueryBuilders.boolQuery();
            //第一次不加1
            if(i>0){
                calendar.add(calendar.DATE, 1);
            }
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
            //bord_flag=11
            QueryBuilder queryBuilder2 = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("bord_flag","11"));
            ((BoolQueryBuilder) query).filter(queryBuilder2);

            //按账号分桶
            TermsAggregationBuilder agg_self_acc_name = AggregationBuilders.terms("agg_self_acc_name").field("self_acc_name");

            agg_self_acc_name.subAggregation(AggregationBuilders.topHits("topHits").size(10000));
            sourceBuilder.query(query);
            sourceBuilder.aggregation(agg_self_acc_name);
            sourceBuilder.size(0);

            searchRequest.source(sourceBuilder);
            //System.out.println("查询条件："+sourceBuilder.toString());
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            //System.out.println("总条数："+searchResponse.getHits().getTotalHits().value);

            //处理聚合结果
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms per_name = aggregations.get("agg_self_acc_name");
            //获取分组后的所有bucket
            List<? extends Terms.Bucket> buckets = per_name.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                //解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();

                ParsedTopHits topHits = bucketAggregations.get("topHits");

                int len = topHits.getHits().getHits().length;
                //System.out.println(len);//一个桶里有多少文档
                Map<String ,Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                String r_date = (String) sourceAsMap.get("date2");//交易日期
                String r_cst_no = (String) sourceAsMap.get("cst_no");//客户号
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");//账号

                //统计各nation重复次数
                HashMap<String, Integer> nation_count_map = new HashMap<String, Integer>();
                //标识该主体的nation重复次数是否大于等于5
                boolean nation_count_rep = false;
                //统计nation重复次数
                for (int j = 0; j < len; j++){
                    Map<String, Object> sourceAsMap2 = topHits.getHits().getHits()[j].getSourceAsMap();
                    String nation = (String) sourceAsMap2.get("nation");
                    if(nation_count_map.containsKey(nation)){
                        //如果该nation已存在，重复次数+1
                        int nation_count = nation_count_map.get(nation);
                        nation_count++;
                        nation_count_map.put(nation,nation_count);
                        if(nation_count>=5){
                            nation_count_rep=true;
                            break;
                        }
                    }else {
                        //如果该nation不存在，就存入，且重复次数为1
                        nation_count_map.put(nation,1);
                    }
                }
                //该主体nation重复次数>=5
                if(nation_count_rep){
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
                        String record1 = "JRSJ-020,"+r_date+","+r_cst_no+","+r_self_acc_name+",,,,";

                    list.add(record1);
//                        System.out.println(record1);
                    }
                }
            }
        }
        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
    }


    //    @SneakyThrows
    @Async
//    @ApiOperation("异步 有返回值")
    @GetMapping("searchAll")
    public void searchAll() throws IOException, ParseException {
//        CompletableFuture<String> createOrder = asyncService.doSomething1("create order");
//        CompletableFuture<String> reduceAccount = asyncService.doSomething2("reduce account");
//        CompletableFuture<String> saveLog = asyncService.doSomething3("save log");
//
//        // 等待所有任务都执行完
//        CompletableFuture.allOf(createOrder, reduceAccount, saveLog).join();
//        // 获取每个任务的返回结果
//        String result = createOrder.get() + reduceAccount.get() + saveLog.get();
//        return result;
        rule_1();
        rule_2();
        rule_3();
        rule_4();
        rule_6();
        rule_7();
        rule_8();
        rule_10();
        rule_12();
        rule_15();
        rule_16();
//        return "over";
    }

}
