package com.soe.caiqihang;


import com.soe.utils.CsvUtil;
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
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;


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

    @Test
    public void age_test() throws ParseException {
        int age = age("20210101","19500101");
        System.out.println(age);
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
        boolean flag = false;
//        3天的窗口可能含有重复结果
//        key:代理人身份证号    value:预警日期
        HashMap<String, String> result = new HashMap<String, String>();
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
            //交易金额>=10000
            QueryBuilder queryBuilder2 = QueryBuilders.rangeQuery("org_amt").gte(10000);
            ((BoolQueryBuilder) query).filter(queryBuilder2);

            //按账户分桶
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no");

//            Map<String, String> bucketsPath = new HashMap<>();
//            bucketsPath.put("sum_org_amt","sum_org_amt");
//            bucketsPath.put("count","_count");
//            //三日交易金额≥10000 且 交易金额是10000的整数倍的整万
//            Script script = new Script("params.sum_rmb_amt >= 10000 && params.sum_rmb_amt % 10000 == 0");
//            BucketSelectorPipelineAggregationBuilder bs = PipelineAggregatorBuilders.bucketSelector("filterAgg", bucketsPath, script);
//            agg_self_acc_no.subAggregation(bs);
            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(10000));
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
                System.out.println(len);
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
                    double rmb_amt = (Double) sourceAsMap2.get("rmb_amt");
                    if((rmb_amt % (int)rmb_amt == 0) && ((int) rmb_amt % 10000 ==0 )){
                        //根据Lend_flag判断 收 / 付
                        String lend_flag = (String) sourceAsMap2.get("lend_flag");
                        String part_acc_no = (String) sourceAsMap2.get("part_acc_no");
                        if(lend_flag.equals("10")){ //收
                            lend1_amt = lend1_amt + (Double) sourceAsMap2.get("rmb_amt");
                            lend1_count++;
                        }else if(lend_flag.equals("11")){//付
//                            lend2_amt = lend2_amt + (Double) sourceAsMap2.get("rmb_amt");
                            lend2_amt = lend2_amt + rmb_amt;
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
                        String record = "JRSJ-016,"+r_date+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);

//                    list.add(record);
                        System.out.println(record);
                    }
                }



            }

//            for (Terms.Bucket bucket : buckets) {
//                //解析bucket
//                Aggregations bucketAggregations = bucket.getAggregations();
//                ParsedTopHits topHits = bucketAggregations.get("topHits");
//                SearchHit[] hits = searchResponse.getHits().getHits();
//                int len = topHits.getHits().getHits().length;
//                System.out.println(len);
//                for (int j = 0; j < len; j++) {
//                    Map<String, Object> sourceAsMap = topHits.getHits().getHits()[j].getSourceAsMap();
//                    // 打印出统计后的数据
//                    System.out.println(sourceAsMap);
//
//                }
//            }
        }
    }


    /**
     * 判断该double是否为整数 （小数部分是否为0）
     */
    @Test
    public void isInt()
    {
        double b = 1000.00;
        int b1 = (int)1000.00;
        if(b % b1 == 0)
            System.out.println(1);
        else
            System.out.println(0);
//        System.out.println(30000.00 % 10000);
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
            //只取具有代理关系的
            QueryBuilder queryBuilder2 = QueryBuilders.termQuery("open_type1","11");
            ((BoolQueryBuilder) query).filter(queryBuilder2);
            //agent_no不为空
            QueryBuilder queryBuilder3 = QueryBuilders.termQuery("agent_no","@N");
            ((BoolQueryBuilder) query).mustNot(queryBuilder3);

            //按账号分桶
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no");

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
                        System.out.println(record1);
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
//        list = removeDuplicationByHashSet(list);
//        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
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
    @Test
    public void rule_18_test() throws ParseException, IOException {
        List<String> list = new ArrayList<>();
        //获取最大和最小日期范围
        String[] min_max = get_Min_Max("tb_acc_txn","date2",null);
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
            calendar.add(calendar.DATE,1);
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
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no");
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
                    double rmb_amt = (Double) sourceAsMap1.get("rmb_amt"); //折人民币交易金额
                    String lend_flag = (String) sourceAsMap1.get("lend_flag"); //资金收付标识
                    if (lend_flag.equals("10")) {//收
                        lend1_amt = lend1_amt + (Double) sourceAsMap1.get("rmb_amt");
                        lend1_count++;
                    } else if (lend_flag.equals("11")) {//付
                        lend2_amt = lend2_amt + rmb_amt;
                        lend2_count++;
                    }

                }
                calendar3.setTime(sdf.parse(r_date));
                calendar3.add(calendar3.DATE, 1); //预警日期：为筛出数据里最大交易日期+1天
                String record1 = "JRSJ-018,"+sdf2.format(calendar3.getTime())+","+r_cst_no+","+r_self_acc_name+ "," + String.format("%.2f", lend1_amt) + "," + String.format("%.2f", lend2_amt) + "," + String.valueOf(lend1_count) + "," + String.valueOf(lend2_count);
                list.add(record1);
                System.out.println(record1);

            }

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
    @Test
    public void rule_19_test(){
        try{
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://202.118.11.39:3306/ccf41_cp?characterEncoding=UTF-8";
            Connection conn = DriverManager.getConnection(url,"soe","soe");
            List<String> list = new ArrayList<>();
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
                String union_query = "SELECT tb_cst_unit.Id_no2 as tat_id_no,tb_acc_txn.Lend_flag as tat_lend_flag, tb_acc_txn.Rmb_amt as tat_rmb_amt, tb_acc_txn.Cst_no as tat_cst_no, tb_acc_txn.Date as tat_date," +
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
                    Double lend_amt = union_res.getDouble("tat_rmb_amt");
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
            Calendar calendar3 = new GregorianCalendar();
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
                for (int j = 0; j <len ;j++){
                    Map<String, Object> sourceAsMap2 = topHits.getHits().getHits()[j].getSourceAsMap();
                    String nation = (String) sourceAsMap2.get("nation");
                    if(nation_count_map.containsKey(nation)){
                        //如果该nation已存在，重复次数+1
                        int nation_count = nation_count_map.get(nation);
                        nation_count++;
                        nation_count_map.put(nation,nation_count);
                        if(nation_count>=5){
                            nation_count_rep=true;
                        }
                    }else {
                        //如果该nation不存在，就存入，且重复次数为1
                        nation_count_map.put(nation,1);
                    }

                }

                //该主体nation重复次数>=5
                if(nation_count_rep) {
                    boolean isNew = true;
                    if (result.containsKey(r_cst_no)) {
                        String exist_date = result.get(r_cst_no);
                        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
                        if (daysBetween(sdf.parse(r_date), sdf.parse(exist_date)) >= 3) {
//                        更新value
                            result.put(r_cst_no, r_date);
                        } else {
                            isNew = false;
                        }
                    } else {
                        result.put(r_cst_no, r_date);
                    }
                    if (isNew) {

                        String record = "JRSJ-020," + r_date + "," + r_cst_no + "," + r_self_acc_name;

//                    list.add(record);
                        System.out.println(record);
                    }
                }
            }
        }
    }


}


