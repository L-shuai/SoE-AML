package com.soe.hanyike;

import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;
//import com.sun.java.browser.plugin2.DOM;
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
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.aggregations.pipeline.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import static com.soe.utils.ESUtils.daysBetween;

@SpringBootTest
class ESForRule11_15 {

    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public ESForRule11_15(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }
    //static List<String> holiday = new ArrayList<>();
    static List<String> holiday_begin = new ArrayList<>();
    static List<String> holiday_end = new ArrayList<>();
    public static void initHoliday_begin(){
        holiday_begin.add("2021-02-10");//春节
        holiday_begin.add("2021-02-25");//元宵
        holiday_begin.add("2021-04-02");//清明
        holiday_begin.add("2021-06-11");//端午
        holiday_begin.add("2021-08-13");//七夕节
        holiday_begin.add("2021-01-21");//中元节
        holiday_begin.add("2021-09-18");//中秋
        holiday_begin.add("2021-10-13");//重阳节
    }
    public static void initHoliday_end(){
        holiday_end.add("2021-02-18");//春节
        holiday_end.add("2021-02-27");//元宵
        holiday_end.add("2021-04-06");//清明
        holiday_end.add("2021-06-15");//端午
        holiday_end.add("2021-08-15");//七夕节
        holiday_end.add("2021-08-23");//中元节
        holiday_end.add("2021-09-22");//中秋
        holiday_end.add("2021-10-15");//重阳节
    }
    /*
    public static void initHoliday() {
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
    */
    private static long daysBetween(Date one, Date two) {
        long difference = (one.getTime() - two.getTime()) / 86400000;
        return Math.abs(difference);
    }

    public String[] get_Min_Max(String index, String field, QueryBuilder queryBuilder) {
        SearchRequest searchRequest = new SearchRequest(index);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        if (queryBuilder != null) {
            searchSourceBuilder.query(queryBuilder);
        } else {
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
        return new String[]{min.getValueAsString(), max.getValueAsString()};
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
    @Test
    public void rule_11_test() throws ParseException, IOException {
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
            //agent_no不能为空
            QueryBuilder queryBuilder3 = QueryBuilders.termQuery("agent_no", "@N");
            ((BoolQueryBuilder) query).mustNot(queryBuilder3);
            //嵌套子聚合查询
            TermsAggregationBuilder agg_cst_no = AggregationBuilders.terms("agg_acc_no").field("self_acc_no")
                    .subAggregation(AggregationBuilders.count("count_acc_no").field("self_acc_no"));
            agg_cst_no.subAggregation(AggregationBuilders.topHits("topHits").size(30000));
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
                    Double M_rmb_amt = (Double) sourceAsMap1.get("rmb_amt");
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
                            sum = sum_money.get(M_key) + M_rmb_amt;
                            sum_money.put(M_key, sum);
                            if (sum > max_sum) {
                                max_sum = sum;
                            }
                        } else {
                            sum_money.put(M_key, M_rmb_amt);
                        }

                        String lend_flag = (String) sourceAsMap1.get("lend_flag");
                        if (lend_flag.equals("10")) {
                            lend1_amt += M_rmb_amt;
                            lend1_count++;
                        } else if (lend_flag.equals("11")) {
                            lend2_amt += M_rmb_amt;
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
        //return list;
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

    @Test
    public void rule_13_test() throws ParseException, IOException {
        List<String> list = new ArrayList<>();
        initHoliday_begin();
        initHoliday_end();
        int festival_len = holiday_begin.size();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        HashMap<String, String> result = new HashMap<String, String>();
        SearchRequest searchRequest = new SearchRequest("tb_acc_txn");
        for(int i = 0; i < festival_len; i++) {
            //Calendar calendar = new GregorianCalendar();
            //calendar.setTime(sdf.parse(holiday_begin.get(i)));
            //Calendar calendar2 = new GregorianCalendar();
            //calendar2.setTime(sdf.parse(holiday_end.get(i)));
            String bDate = holiday_begin.get(i);
            String eDate = holiday_end.get(i);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            QueryBuilder query = QueryBuilders.boolQuery();
            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("date2").format("yyyy-MM-dd").gte(bDate).lte(eDate);
            ((BoolQueryBuilder) query).filter(queryBuilder1);

            TermsAggregationBuilder agg_cst_no = AggregationBuilders.terms("agg_acc_no").field("cst_no")
                    .subAggregation(AggregationBuilders.count("count_acc_no").field("cst_no"))
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
            System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);  //这里并不是topHits的数量

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
                        lend1_amt = lend1_amt + (Double) sourceAsMap2.get("rmb_amt");
                        lend1_count++;
                    } else if (lend_flag.equals("11")) {//付
                        lend2_amt = lend2_amt + (Double) sourceAsMap2.get("rmb_amt");
                        lend2_count++;
                    }
                    String trade_date = (String) sourceAsMap2.get("date2");
                    Date new_date = sdf.parse(trade_date);
                    if (max_date.compareTo(new_date) < 0) {
                        max_date = new_date;
                    }
                }
                String record = "JRSJ-013," + sdf.format(max_date) + "," + r_cst_no + "," + r_self_acc_name + "," + String.format("%.2f", lend1_amt) + "," + String.format("%.2f", lend2_amt) + "," + String.valueOf(lend1_count) + "," + String.valueOf(lend2_count);
                System.out.println(record);
                list.add(record);
            }
            //return list;
        }
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
    @Test
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
            calendar.add(calendar.DATE, 1);
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
            TermsAggregationBuilder agg_self_acc_name = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no")
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
                //获取该客户的注册资金
                Double reg_amt = 0.0;//初始为0
                reg_amt = reg_amt_map.get(r_cst_no);

                //总交易金额
                Sum sum_org_amt =  bucketAggregations.get("sum_org_amt");
                //若总交易金额不大于注册本金，则没必要遍历该桶
                if(sum_org_amt.getValue() < reg_amt){
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
                        lend1_amt = lend1_amt + (Double) sourceAsMap2.get("rmb_amt");
                        lend1_count++;
                    }else if(lend_flag.equals("11")){//付
                        lend2_amt = lend2_amt + (Double) sourceAsMap2.get("rmb_amt");
                        lend2_count++;
                    }

                }
                String record = "JRSJ-014,"+r_date+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);
                list.add(record);
                System.out.println(record);
                System.out.println("注册资金："+reg_amt);

            }
//            restHighLevelClient.close();

        }

//        }
//        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        System.out.println("rule_14 : end");
//        return list;
    }

    @Test
    public void rule_14_test() throws IOException, ParseException{
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
                String  cst_no_query = "SELECT tb_acc_txn.Cst_no as tbt_cst_no from tb_acc_txn JOIN tb_cst_unit ON tb_acc_txn.Cst_no = tb_cst_unit.Cst_no" +
                        " where tb_acc_txn.Org_amt >= tb_cst_unit.Reg_amt and tb_acc_txn.Org_amt >= 500000 and tb_acc_txn.Date = '"+curDay+"'"+
                        "GROUP BY tb_acc_txn.Cst_no";
                ResultSet res = smt.executeQuery(cst_no_query);
                List<String> cst_no_list = new ArrayList<>();
                //将每日分组后的账户添加到list中
                while(res.next()) {
                    String acc_no = res.getString("tbt_cst_no");
                    cst_no_list.add(acc_no);
                }
                res.close();
                //从list中取出每个账户，并按条件查询每日该账户的记录
                for(int j = 0; j<cst_no_list.size();j++){
                    //按照题目描述做多表查询操作
                    String union_query = "SELECT tb_acc_txn.Lend_flag as tat_lend_flag, tb_acc_txn.Rmb_amt as tat_rmb_amt, tb_acc_txn.Cst_no as tat_cst_no, tb_acc_txn.Date as tat_date," +
                            "tb_acc_txn.Self_acc_name as tat_self_acc_name from tb_acc_txn JOIN tb_cst_unit ON tb_acc_txn.Cst_no = tb_cst_unit.Cst_no" +
                            " where tb_acc_txn.Org_amt >= tb_cst_unit.Reg_amt and tb_acc_txn.Org_amt >= 500000 and tb_acc_txn.Date = '"+curDay+"'"+
                            "and tb_acc_txn.Cst_no = "+cst_no_list.get(j);
                    ResultSet union_res = smt.executeQuery(union_query);
                    Date date_max = sdf.parse("1999-01-01");
                    String r_self_acc_name = "";
                    Calendar calendar1 = new GregorianCalendar();
                    String r_cst_no = cst_no_list.get(j);
                    boolean out_flag = false;
                    //收款总金额
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
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

/*
    @Test
    public void rule_13_test() throws ParseException, IOException {
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
            TermsAggregationBuilder agg_cst_no = AggregationBuilders.terms("agg_acc_no").field("self_acc_no")
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
                        lend1_amt = lend1_amt + (Double) sourceAsMap2.get("rmb_amt");
                        lend1_count++;
                    } else if (lend_flag.equals("11")) {//付
                        lend2_amt = lend2_amt + (Double) sourceAsMap2.get("rmb_amt");
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
//                String record = "JRSJ-013," + sdf.format(calendar4.getTime()) + "," + r_cst_no + "," + r_self_acc_name + "," + String.format("%.2f", lend1_amt) + "," + String.format("%.2f", lend2_amt) + "," + String.valueOf(lend1_count) + "," + String.valueOf(lend2_count);
                String record = "JRSJ-013," + sdf.format(max_date) + "," + r_cst_no + "," + r_self_acc_name + "," + String.format("%.2f", lend1_amt) + "," + String.format("%.2f", lend2_amt) + "," + String.valueOf(lend1_count) + "," + String.valueOf(lend2_count);
                System.out.println(record);
                list.add(record);
            }
        }
        //return list;
    }
 */
}