package com.soe.config;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;
import org.springframework.stereotype.Component;

/**
 * ElasticSearch 客户端配置
 *
 * @author lishuai
 * 2022/08/18
 */
@Configuration
public class RestClientConfig {
    @Value("${elasticsearch.host}")
    private String host;

//    @Override
    @Bean
    @Qualifier("restHighLevelClient")
    public RestHighLevelClient restHighLevelClient() {
        // RestHighLevelClient highLevelClient = new RestHighLevelClient(
        // RestClient.builder(new HttpHost("127.0.0.1", 9200, "http")));

        RestHighLevelClient highLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("202.118.11.39", 9200, "http"))  //服务器端数据源
//                RestClient.builder(new HttpHost("localhost", 9200, "http"))  //本地数据源
                        .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                            // 该方法接收一个RequestConfig.Builder对象，对该对象进行修改后然后返回。
                            @Override
                            public RequestConfig.Builder customizeRequestConfig(
                                    RequestConfig.Builder requestConfigBuilder) {
                                return requestConfigBuilder.setConnectTimeout(5000 * 1000) // 连接超时（默认为1秒）
                                        .setSocketTimeout(9000 * 1000);// 套接字超时（默认为30秒）//更改客户端的超时限制默认30秒现在改为100*1000分钟
                            }
                        }));// 调整最大重试超时时间（默认为30秒）.setMaxRetryTimeoutMillis(60000);

        return highLevelClient;
    }

}
