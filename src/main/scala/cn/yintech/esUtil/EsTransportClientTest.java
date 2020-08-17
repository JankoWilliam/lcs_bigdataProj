package cn.yintech.esUtil;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class EsTransportClientTest {

    final private static String[] IP = {"115.28.252.78"};
    final private static String[] INDEX = {"real_time_count"};

    public static void main(String[] args) {
        TransportClient client = getTransportClient();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("uid", "27243518"));
        RangeQueryBuilder range = QueryBuilders.rangeQuery("logtime").gte("2020-02-25T18:30:14+08:00");
//        boolQueryBuilder.must(range);

        SearchResponse scrollResp = client.prepareSearch(INDEX)
                .setScroll(new TimeValue(3000))
//                .setQuery(boolQueryBuilder)
                .setSize(10000) // 设置条数
                .execute().actionGet();
        System.out.println("hits:" + scrollResp.getHits().totalHits);
//        for (SearchHit hit : scrollResp.getHits().getHits()){
//
//        }

        client.close();
    }

    public static TransportClient getTransportClient() {
        // 集群设置
        Settings settings = Settings.builder()
                .put("cluster.name", "licaishi_1")//集群名称
                .build();
        TransportClient client = new PreBuiltTransportClient(settings);
        //添加集群地址和tcp服务端口 IP是由集群的各个node的ip组成的数组
        try {
            for (String ip : IP) {
                client.addTransportAddresses(new TransportAddress(InetAddress.getByName(ip), 9300));
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }
}
