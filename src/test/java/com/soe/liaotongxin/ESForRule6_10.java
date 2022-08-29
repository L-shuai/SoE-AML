package com.soe.liaotongxin;
import java.sql.*;
import com.soe.utils.CsvUtil;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
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
import java.util.Date;

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
    public Map<String,Integer> get_cred_txn(String bDate,String eDate) throws IOException, ParseException {
        //返回map, key:客户号   value：注册资金
        Map<String,Integer> result = new HashMap<>();
        SearchRequest searchRequest = new SearchRequest("tb_cred_txn");//指定搜索索引
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();//指定条件对象
        QueryBuilder query = QueryBuilders.boolQuery();
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");

        Date date_b=sdf1.parse(bDate);
        Date date_e=sdf1.parse(eDate);

        String bDate_new = sdf2.format(date_b);
        String eDate_new = sdf2.format(date_e);
        QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("date").format("yyyyMMdd").gte(bDate_new).lte(eDate_new);
        ((BoolQueryBuilder) query).filter(queryBuilder1);

        //资金收付标识为付
        QueryBuilder queryBuilder2 = QueryBuilders.termQuery("lend_flag","11");
        ((BoolQueryBuilder) query).filter(queryBuilder2);
        QueryBuilder queryBuilder3 = QueryBuilders.termQuery("pow_owner","@N");
        ((BoolQueryBuilder) query).mustNot(queryBuilder3);
        sourceBuilder.query(query).fetchSource(new String[]{"pos_owner","date"}, new String[]{});//查询tb_cred_txn 表的pos_owner字段
        sourceBuilder.size(100000);
        searchRequest.source(sourceBuilder);//指定查询条件

        //参数1：搜索的请求对象，   参数2：请求配置对象   返回值：查询结果对象
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        System.out.println("总条数："+searchResponse.getHits().getTotalHits().value);
        //获取结果
        SearchHit[] hits = searchResponse.getHits().getHits();
        for(SearchHit hit:hits){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String pos_owner = (String) sourceAsMap.get("pos_owner");
            result.put(pos_owner,1);
        }
        return result;
    }
    public Map<String,Integer> get_cst_unit() throws IOException {
        //返回List<String>：rep_name:法人姓名
        Map<String,Integer> result = new HashMap<>();
        SearchRequest searchRequest = new SearchRequest("tb_cst_unit");//指定搜索索引
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();//指定条件对象
        sourceBuilder.query(QueryBuilders.matchAllQuery()).fetchSource(new String[]{"rep_name"}, new String[]{});//查询rep_name字段

        sourceBuilder.size(100000);
        searchRequest.source(sourceBuilder);//指定查询条件

        //参数1：搜索的请求对象，   参数2：请求配置对象   返回值：查询结果对象
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        System.out.println("总条数："+searchResponse.getHits().getTotalHits().value);
        //获取结果
        SearchHit[] hits = searchResponse.getHits().getHits();
        for(SearchHit hit:hits){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String r_name = (String) sourceAsMap.get("rep_name");
            result.put(r_name,1);
//            System.out.println(r_name);
        }
        return result;
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
            calendar.add(calendar.DATE, 1);
            String curDay = sdf.format(calendar.getTime());

            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
            //过滤日期
            queryBuilder.filter(QueryBuilders.termQuery("date2", curDay));
            //过滤资金收付为收
            queryBuilder.filter(QueryBuilders.termQuery("lend_flag", "10"));
            //按账户名进行分桶
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no")
                    .subAggregation(AggregationBuilders.cardinality("count_part_acc_no").field("part_acc_no"))
                    .subAggregation(AggregationBuilders.sum("sum_rmb_amt").field("rmb_amt"));

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
                System.out.println(len);
                String r_date = (String) sourceAsMap.get("date2");
//                折人民币交易金额-收
                Sum r_lend1 = bucketAggregations.get("sum_rmb_amt");
//                折人民币交易金额-付
                String r_lend2 = "0";
//                客户号
                String r_cst_no = (String) sourceAsMap.get("cst_no");
//                客户名称
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
                //写入到csv文件，注意各列对其，用英文逗号隔开
                //规则代码,预警日期,客户号,客户名称,折人民币交易金额-收,折人民币交易金额-付,交易笔数收,交易笔数付
                String record = "JRSJ-001,"+r_date+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",r_lend1.getValueAsString())+","+String.format("%.2f",r_lend2)+String.valueOf(len)+","+"0";
                list.add(record);
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
        List<String> list = new ArrayList<>();
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
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no");



            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(1000));
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
                        lend1_amt = lend1_amt + (Double.parseDouble(sourceAsMap1.get("rmb_amt").toString())) ;
                        lend1_count++;
                    }else if(lend_flag.equals("11")){//付
                        lend2_amt = lend2_amt + (Double.parseDouble(sourceAsMap1.get("rmb_amt").toString())) ;
                        lend2_count++;
                    }
                }
                if(three_days_transaction_count >= len * 0.6){
                    String record = "JRSJ-007,"+r_date+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);
                    list.add(record);
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
        List<String> list = new ArrayList<>();
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
            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no").size(80)
                            .subAggregation(AggregationBuilders.count("total_bank_name_count").field("part_bank_name"));

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

//            System.out.println("查询条件：" + searchSourceBuilder.toString());
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//            System.out.println("总条数：" + searchResponse.getHits().getTotalHits().value);

            Aggregations aggregations = searchResponse.getAggregations();

            ParsedTerms txn_per_day = aggregations.get("agg_self_acc_no");
            // 获取到分组后的所有bucket
            List<? extends Terms.Bucket> buckets = txn_per_day.getBuckets();
            System.out.println(buckets.size());
            for (Terms.Bucket bucket : buckets) {
                // 解析bucket
                Aggregations bucketAggregations = bucket.getAggregations();
                ParsedTopHits topHits = bucketAggregations.get("topHits");
                Map<String, Object> sourceAsMap = topHits.getHits().getHits()[0].getSourceAsMap();
                ValueCount  total_transaction = bucketAggregations.get("total_bank_name_count");
                int total_transaction_count = (int) total_transaction.value();
                int len = total_transaction_count;

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
                    Double transaction_money = (Double) sourceAsMap1.get("rmb_amt");
                    if(bank_name.contains("邮")){
                        youchu_count += 1;
                        youchu_money += transaction_money;
                    }
                    if(bank_name.contains("农业")){
                        nongye_count += 1;
                        nongye_money += transaction_money;
                    }
                    if(bank_name.contains("信用社")){
                        xinyongshe_count += 1;
                        xinyongshe_money += transaction_money;
                    }
                    String lend_flag = (String) sourceAsMap1.get("lend_flag");
                    if(lend_flag.equals("10")){ //收
                        lend1_amt = lend1_amt + (Double.parseDouble(sourceAsMap1.get("rmb_amt").toString())) ;
                        lend1_count++;
                    }else if(lend_flag.equals("11")){//付
                        lend2_amt = lend2_amt + (Double.parseDouble(sourceAsMap1.get("rmb_amt").toString())) ;
                        lend2_count++;
                    }
                }
                if(youchu_count + nongye_count + xinyongshe_count >= len *0.5 && youchu_money + nongye_money +xinyongshe_money >= 500000){
                    String record = "JRSJ-008,"+r_date+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);
                    System.out.println(record);
                    list.add(record);
                }
            }
        }
        System.out.println(list.size());

    }
    @Test
    public void rule_9_test() throws IOException, ParseException{
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
                            "tb_acc_txn.Rmb_amt as tat_rmb_amt from tb_cred_txn, tb_acc_txn,tb_cst_unit where " +
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
            // 关闭流 (先开后关)
            smt.close();
            conn.close();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
    @Test
    public void rule_9_new() throws IOException, ParseException {
        System.out.println("rule_9 : begin");

        List<String> list = new ArrayList<>();
        //仅 公司账户
        String[] min_max = get_Min_Max("tb_acc_txn", "date2",null);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long daysBetween = daysBetween(sdf.parse(min_max[1]),sdf.parse(min_max[0]));
        Calendar calendar = new GregorianCalendar();
//        calendar.setTime(sdf.parse(min_max[0]));
        calendar.setTime(sdf.parse("2021-01-12"));

        Calendar calendar2 = new GregorianCalendar();
        //获取客户对应的注册资金map

        Map<String,Integer> rep_map = get_cst_unit();

        for (int i=0;i<daysBetween;i++) {

            SearchRequest searchRequest = new SearchRequest("tb_acc_txn");

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
            Map<String,Integer> cred_map = get_cred_txn(bDate,eDate);
            //3天为窗口
            QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("date2").format("yyyy-MM-dd").gte(bDate).lte(eDate);
//            QueryBuilder queryBuilder1 = QueryBuilders.termQuery("date2",curDay);
            ((BoolQueryBuilder) query).filter(queryBuilder1);
//        //公司账户
            QueryBuilder queryBuilder2 = QueryBuilders.termQuery("lend_flag", "11");
            ((BoolQueryBuilder) query).filter(queryBuilder2);



            //嵌套子聚合查询  以self_acc_name账户名称分桶
            TermsAggregationBuilder agg_self_acc_name = AggregationBuilders.terms("agg_cst_no").field("cst_no");

            agg_self_acc_name.subAggregation(AggregationBuilders.topHits("topHits").size(30000));
            searchSourceBuilder.aggregation(agg_self_acc_name);
            searchSourceBuilder.size(0);


            searchSourceBuilder.query(query);

            searchRequest.source(searchSourceBuilder);
//            System.out.println("查询条件：" + searchSourceBuilder.toString());

            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            Aggregations aggregations = searchResponse.getAggregations();

            ParsedTerms txn_per_day = aggregations.get("agg_cst_no");
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
                String r_cst_no = (String) sourceAsMap.get("cst_no");
                String r_self_acc_name = (String) sourceAsMap.get("self_acc_name");
                String r_part_acc_name = (String) sourceAsMap.get("part_acc_name");
                boolean cred_flag = false;
                boolean cst_unit_flag = false;
                SimpleDateFormat sdf1= new SimpleDateFormat("yyyyMMdd");
                Date date_max = sdf.parse("1999-01-01");
                List<String> acc_name_list = new ArrayList<>();
                List<String> part_acc_name_list = new ArrayList<>();
                String final_name= "";
                //遍历单个cst_no桶内的所有数据，得到所有不同的self_acc_name、得到最大的日期
                for (int j = 0;j<len;j++){
                    Map<String, Object> sourceAsMap1 = topHits.getHits().getHits()[j].getSourceAsMap();
                    String date_per_row = (String) sourceAsMap1.get("date2");
                    String acc_name = (String) sourceAsMap1.get("self_acc_name");
                    String part_acc_name = (String) sourceAsMap1.get("part_acc_name");
                    if(acc_name_list.contains(acc_name) == false){
                        acc_name_list.add(acc_name);
                    }
                    if(part_acc_name_list.contains(part_acc_name) == false){
                        part_acc_name_list.add(part_acc_name);
                    }
                    Date new_date = sdf.parse(date_per_row);
                    if(date_max.compareTo(new_date) < 0 ){
                        date_max = new_date;
                    }

                }
                if(acc_name_list.size()>1){
                    System.out.println(acc_name_list.size());
                }
                //遍历同一cst_no下的不同self_acc_name，如果包含在cred_map或rep_map则输出
                for(int name_idx = 0;name_idx<acc_name_list.size();name_idx++){
                    if(cred_map.containsKey(acc_name_list.get(name_idx))){
                        final_name = acc_name_list.get(name_idx);
                        cred_flag = true;
                        break;
                    }
                }
                for(int part_idx = 0;part_idx<part_acc_name_list.size();part_idx++){
                    if(rep_map.containsKey(part_acc_name_list.get(part_idx))){
                        cst_unit_flag = true;
                        break;
                    }
                }
                if((cred_flag || cst_unit_flag) == false){
                    continue;
                }
                if(final_name!=""){
                    r_self_acc_name = final_name;
                }
                String record = "JRSJ-009,"+sdf.format(date_max)+","+r_cst_no+","+r_self_acc_name+",,,,";
                list.add(record);
                System.out.println(record);

            }
//            restHighLevelClient.close();

        }

//        }
//        CsvUtil.writeToCsv(headDataStr, list, csvfile, true);
        System.out.println("rule_009 : end");
//        return list;
    }
    @Test
    public void rule_10_test() throws IOException, ParseException{
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
                Sum sum = bucketAggregations.get("sum_rmb_amt");
                System.out.println(sum.getValueAsString());
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
                        lend1_amt = lend1_amt + (Double.parseDouble(sourceAsMap1.get("rmb_amt").toString())) ;
                        lend1_count++;
                    }else if(lend_flag.equals("11")){//付
                        lend2_amt = lend2_amt + (Double.parseDouble(sourceAsMap1.get("rmb_amt").toString())) ;
                        lend2_count++;
                    }
                }
                String record = "JRSJ-010,"+r_date+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);
                System.out.println(record);
                list.add(record);
            }
        }
    }
    @Test
    public void rule_11_test() throws IOException, ParseException {
        List<String> list = new ArrayList<>();
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
//            QueryBuilder queryBuilder2 = QueryBuilders.termQuery("agent_name", "@N");
//            ((BoolQueryBuilder) query).mustNot(queryBuilder2);
            QueryBuilder queryBuilder3 = QueryBuilders.termQuery("agent_no", "@N");
            ((BoolQueryBuilder) query).mustNot(queryBuilder3);

            TermsAggregationBuilder agg_self_acc_no = AggregationBuilders.terms("agg_self_acc_no").field("self_acc_no")
                    .subAggregation(AggregationBuilders.count("count_part_acc_no").field("part_acc_no"));

            agg_self_acc_no.subAggregation(AggregationBuilders.topHits("topHits").size(1000));
            searchSourceBuilder.aggregation(agg_self_acc_no);
            searchSourceBuilder.size(0);
            searchSourceBuilder.query(query);
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
                ValueCount total_transaction = bucketAggregations.get("count_part_acc_no");
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
                int total_transaction_count = (int) total_transaction.value();
                Map<String,Integer> transaction_count_dict = new HashMap<>();
                Map<String,Double> transaction_amt_dict = new HashMap<>();
                int max_count = 0;
                double max_amt = 0;
                for(int j = 0; j<len;j++){
                    Map<String, Object> sourceAsMap1 = topHits.getHits().getHits()[j].getSourceAsMap();
                    String transaction_agent_name = (String) sourceAsMap1.get("agent_name");
                    String transaction_agent_no = (String) sourceAsMap1.get("agent_no");
                    Double transaction_rmb_amt = (Double) sourceAsMap1.get("rmb_amt");
                    String agent_info = transaction_agent_name+transaction_agent_no;
                    if(transaction_agent_name != "@N"){
                        if(transaction_count_dict.containsKey(agent_info) == false){
                            transaction_count_dict.put(agent_info, 0);

                        }else{
                            int agent_count = transaction_count_dict.get(agent_info) + 1 ;
                            if(agent_count  > max_count){
                                max_count = agent_count;
                            }
                            transaction_count_dict.put(agent_info, agent_count);
                        }
                        if(transaction_amt_dict.containsKey(agent_info) == false){
                            transaction_amt_dict.put(agent_info,transaction_rmb_amt);
                        }else {
                            Double rmb_amt = transaction_amt_dict.get(agent_info) + transaction_rmb_amt;
                            if(rmb_amt > max_amt){
                                max_amt = rmb_amt;
                            }
                            transaction_amt_dict.put(agent_info, rmb_amt);
                        }
                    }

                    String lend_flag = (String) sourceAsMap1.get("lend_flag");
                    if(lend_flag.equals("10")){ //收
                        lend1_amt = lend1_amt + (Double.parseDouble(sourceAsMap1.get("rmb_amt").toString())) ;
                        lend1_count++;
                    }else if(lend_flag.equals("11")){//付
                        lend2_amt = lend2_amt + (Double.parseDouble(sourceAsMap1.get("rmb_amt").toString())) ;
                        lend2_count++;
                    }
                }
                if(max_amt >= 500000 && max_count >= total_transaction_count * 0.6){
                    String record = "JRSJ-011,"+r_date+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);
                    System.out.println(record);
                    list.add(record);
                }

            }

        }
    }

    @Test
    public void rule_15_test() throws IOException, ParseException {
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
                String union_query = "SELECT tb_acc_txn.Rmb_amt as tat_rmb_amt, tb_acc_txn.Cst_no as tat_cst_no, tb_acc_txn.Self_acc_name as tat_self_acc_name, " +
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
                    String record = "JRSJ-0015,"+sdf.format(calendar1.getTime())+","+r_cst_no+","+r_self_acc_name+","+String.format("%.2f",lend1_amt)+","+String.format("%.2f",lend2_amt)+","+String.valueOf(lend1_count)+","+String.valueOf(lend2_count);
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
        }
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
                    String union_query = "SELECT tb_acc_txn.Lend_flag as tat_lend_flag, tb_cst_unit.Code as tcu_reg_amt,tb_acc_txn.Rmb_amt as tat_rmb_amt, tb_acc_txn.Cst_no as tat_cst_no, tb_acc_txn.Date as tat_date," +
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
                        Double lend_amt = union_res.getDouble("tat_rmb_amt");
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
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}


