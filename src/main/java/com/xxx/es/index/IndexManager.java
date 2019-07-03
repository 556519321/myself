package com.xxx.es.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sound.midi.Soundbank;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class IndexManager {

    private Settings settings;

    private TransportClient transportClient;

    @Before
    public void createSettings() throws UnknownHostException {
        //1、创建一个Settings对象，相当于一个配置信息。主要配置集群的名称。
        settings = Settings.EMPTY;

        //2、创建一个客户端对象
        transportClient = new PreBuiltTransportClient(settings);
        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
        System.out.println(transportClient);
    }

    //1-创建索引库
    @Test
    public void creatIndexResp(){

        //3、使用client对象创建一个索引库。
        CreateIndexResponse indexResponse = transportClient.admin().indices().prepareCreate("blog3").get();
        System.out.println(indexResponse.index());
    }

    /*
    "mappings": {
    "content": {
        "properties": {
            "id": {
                "type": "text",
                        "fields": {
                    "keyword": {
                        "ignore_above": 256,
                                "type": "keyword"
                    }
                }
            },
            "title": {
                "type": "text",
                        "fields": {
                    "keyword": {
                        "ignore_above": 256,
                                "type": "keyword"
                    }
                }
            },
            "content": {
                "type": "text",
                        "fields": {
                    "keyword": {
                        "ignore_above": 256,
                                "type": "keyword"
                    }
                }
            }
        }
    }*/
    //2：创建规则
    @Test
    public void createMapping() throws IOException {
        CreateIndexResponse indexResponse = transportClient.admin().indices().prepareCreate("blog1").get();
        if(indexResponse.isAcknowledged()){
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("content")
                    .startObject("properties")

                    .startObject("id").field("type", "long").endObject()
                    // 细粒度切分使用 ik_max_word  最小切分 ik_smart
                    .startObject("title").field("type", "text").field("store",true).field("analyzer", "ik_smart").endObject()
                    .startObject("content").field("type", "text").field("store", true).field("analyzer", "ik_smart").endObject()

                    .endObject()
                    .endObject()
                    .endObject();
            PutMappingResponse putMappingResponse = transportClient.admin().indices().preparePutMapping("blog1").setType("content").setSource(contentBuilder).get();
            System.out.println(putMappingResponse.isAcknowledged());
        }

    }

    // 3:添加/修改文档--方式一XContentBuilder
    @Test
    public void createIndex() throws IOException {
        // 转变json数据
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("id","1")
                .field("title","elasticsearch是一个基于lucene的搜索服务 update")
                .field("content","ElasticSearch是一个基于Lucene的搜索服务器。\" +\n" +
                        "                        \"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。\" +\n" +
                        "                        \"Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，\" +\n" +
                        "                        \"是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，\" +\n" +
                        "                        \"可靠，快速，安装使用方便。")
                .endObject();
        // 创建索引
        IndexResponse indexResponse = transportClient.prepareIndex("blog3","content","1")
                .setSource(contentBuilder).get();
        System.out.println(indexResponse.status());
    }

    // 3:添加/修改文档--方式二实体类
    @Test
    public void createIndexByPojo() throws JsonProcessingException {
        Content content = new Content();
        content.setId(2);
        content.setTitle("solr是一个基于lucene的搜索服务");
        content.setContent("solr是一个基于Lucene的搜索服务器。它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便");

        //转成json字符串
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonStr = objectMapper.writeValueAsString(content);

        //创建索引,主键id相同就是修改
        IndexResponse indexResponse = transportClient.prepareIndex("blog1","content",content.getId()+"")
                .setSource(jsonStr).get();
        System.out.println(indexResponse.status());

    }



    //删除文档
    @Test
    public void deleteIndex(){
        DeleteResponse deleteResponse = transportClient.prepareDelete("blog3","content","1").get();
        System.out.println(deleteResponse.status());
    }



    @After
    public void closeSource(){
        //4、关闭client对象。
        transportClient.close();
    }
}
