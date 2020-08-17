package cn.yintech.esUtil;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EsHighRrestClientTest {

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = ESConfig.client();

        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("uid", "27243518");//这里可以根据字段进行搜索，must表示符合条件的，相反的mustnot表示不符合条件的
//        boolBuilder.must(matchQueryBuilder);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("logtime"); //新建range条件
        rangeQueryBuilder.gte("now-2h"); //开始时间
        boolBuilder.must(rangeQueryBuilder);
//        rangeQueryBuilder.gte("2020-02-24T18:00:00.000Z"); //开始时间
//        rangeQueryBuilder.lte("2020-02-25T23:59:59.999Z"); //结束时间
//        rangeQueryBuilder.lte("2020-02-25T23:59:59.999Z"); //结束时间

        SortBuilder sortBuilder = SortBuilders.fieldSort("logtime").order(SortOrder.ASC);// 排训规则

        sourceBuilder.query(boolBuilder); //设置查询，可以是任何类型的QueryBuilder。
        sourceBuilder.from(0); //设置确定结果要从哪个索引开始搜索的from选项，默认为0
        sourceBuilder.size(10000); //设置确定搜素命中返回数的size选项，默认为10
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)); //设置一个可选的超时，控制允许搜索的时间。
        sourceBuilder.sort(sortBuilder);

        //sourceBuilder.fetchSource(new String[] {"fields.port","fields.entity_id","fields.message"}, new String[] {}); //第一个是获取字段，第二个是过滤的字段，默认获取全部
        SearchRequest searchRequest = new SearchRequest("real_time_count"); //索引
        searchRequest.types("real_time_count"); //类型
        searchRequest.source(sourceBuilder);
        // 分页查询全量数据
        List<SearchHit> searchHits = ESConfig.scrollSearchAll(client, 10L, searchRequest);

        System.out.println(searchHits.size());
//        SearchResponse response = client.search(searchRequest);
//        SearchHits hits = response.getHits();  //SearchHits提供有关所有匹配的全局信息，例如总命中数或最高分数：
//        SearchHit[] searchHits = hits.getHits();
//
//        System.out.println("total hits : " + hits.totalHits);
//        System.out.println("total hits : " + searchHits.length);
//        for (SearchHit hit : searchHits) {
//
//
//
////            System.out.println("search -> " + hit.getSourceAsString());
//        }

        client.close();
    }


}
