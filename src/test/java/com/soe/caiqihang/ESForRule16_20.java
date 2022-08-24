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
                        String record1 = "JRSJ-020,"+r_date+","+r_cst_no+","+r_self_acc_name;

//                    list.add(record);
                        System.out.println(record1);
                    }
                }
            }
        }
    }


}


