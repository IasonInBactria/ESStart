package com.argo;

import com.alibaba.fastjson.JSON;
import com.argo.dao.BookMapper;
import com.argo.domain.Book;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
@Slf4j
class EsStartApplicationTests {

    @Test
    void contextLoads() {
    }

    @Autowired
    private BookMapper bookMapper;

    @Test
    void testBook(){
        bookMapper.selectById(1);
    }

    @BeforeEach
    void setUp() {
        HttpHost host = HttpHost.create("http://localhost:9200");
        RestClientBuilder builder = RestClient.builder(host);
        restHighLevelClient = new RestHighLevelClient(builder);
    }

    @AfterEach
    void tearDown() throws IOException {
        restHighLevelClient.close();
    }

    private RestHighLevelClient restHighLevelClient;
    @Test
    void testCreateClient() throws IOException {

        CreateIndexRequest request = new CreateIndexRequest("books");
        restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);

        restHighLevelClient.close();
    }

    //创建索引
    @Test
    void testCreateIndexByIK() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("books");

        String jsonStr = "{\n" +
                "    \"mappings\": {\n" +
                "        \"properties\": {\n" +
                "            \"id\": {\n" +
                "                \"type\": \"keyword\"\n" +
                "            },\n" +
                "            \"name\": {\n" +
                "                \"type\": \"text\",\n" +
                "                \"analyzer\": \"ik_max_word\",\n" +
                "                \"copy_to\": \"all\"\n" +
                "            },\n" +
                "            \"type\": {\n" +
                "                \"type\": \"keyword\"\n" +
                "            },\n" +
                "            \"decription\": {\n" +
                "                \"type\": \"text\",\n" +
                "                \"analyzer\": \"ik_max_word\",\n" +
                "                \"copy_to\": \"all\"\n" +
                "            },\n" +
                "            \"all\": {\n" +
                "                \"type\": \"text\",\n" +
                "                \"analyzer\": \"ik_max_word\"\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        request.source(jsonStr, XContentType.JSON);
        restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
    }

    //创建文档
    @Test
    void testCreateDoc(){
        Book book = bookMapper.selectById(1);
        IndexRequest request = new IndexRequest("books").id(String.valueOf(1));
        String jsonStr = JSON.toJSONString(book);
        request.source(jsonStr, XContentType.JSON);
    }

    //创建文档
    @Test
    void testCreateAllDoc() throws IOException {
        List<Book> bookList = bookMapper.selectList(null);
        BulkRequest bulkRequest = new BulkRequest();
        for (Book book: bookList){
            IndexRequest indexRequest = new IndexRequest("books").id(book.getId().toString());
            String jsonStr = JSON.toJSONString(book);
            indexRequest.source(jsonStr, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    @Test
    void testGet() throws IOException {
        GetRequest request = new GetRequest("books", "1");
        GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        String responseJson = response.getSourceAsString();
        log.info("Get response:" + responseJson);
    }

    @Test
    void testSearch() throws IOException {
        SearchRequest request = new SearchRequest("books");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.termQuery("name", "java"));
        request.source(builder);

        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        for (SearchHit hit_item: hits){
            String source = hit_item.getSourceAsString();
            Book book = JSON.parseObject(source, Book.class);
            log.info("get result:" + book);
        }
    }

}
