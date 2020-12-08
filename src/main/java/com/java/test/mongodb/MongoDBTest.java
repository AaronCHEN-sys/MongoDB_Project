package com.java.test.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * Description:	   <br/>
 * Date:     2020/12/08 19:48 <br/>
 *
 * @author Aaron CHEN
 * @see
 */
public class MongoDBTest {

    private MongoClient mongoClient = null;
    MongoDatabase database = null;
    MongoCollection<Document> collection = null;

    /**
     * MongoDB连接
     */
    @Before
    public void init() {
        //1.连接MongoDB
        mongoClient = new MongoClient("127.0.0.1", 27017);
        //2.指定连接的库
        database = mongoClient.getDatabase("k8513");
        //3.指定操作的集合
        collection = database.getCollection("users");
    }

    /**
     * 插入一条数据， 方法一
     */
    @Test
    public void test1() {
        Document document = new Document();
        document.append("username", "Lucien");
        document.append("password", "147258369");
        document.append("age", 25);
        document.append("address", "Changsha");
        collection.insertOne(document);
    }

    /**
     * 插入一条数据， 方法二
     */
    @Test
    public void test2() {
        Document document = Document.parse("{\"username\":\"Bob\",\"password\":\"15985364785Bob\",\"age\":27,\"address\":\"Shanghai\"}");
        collection.insertOne(document);
    }

    /**
     * 插入一条数据， 方法二
     */
    @Test
    public void test3() {
        Document document1 = Document.parse("{\"username\":\"Evelyn\",\"password\":\"12Evelyn12\",\"age\":18,\"address\":\"Guangzhou\"}");
        Document document2 = Document.parse("{\"username\":\"Alice\",\"password\":\"Alice12jL\",\"age\":21,\"address\":\"Beijing\"}");
        Document document3 = Document.parse("{\"username\":\"Frank\",\"password\":\"frank5987\",\"age\":24,\"address\":\"Chongqing\"}");
        collection.insertMany(Arrays.asList(document1, document2, document3));
    }

    /**
     * MongoDB性能测试
     */
    @Test
    public void test4() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            Document document = new Document();
            document.append("name", "如花_" + i);
            document.append("age", "22");
            document.append("gender", "女");
            collection.insertOne(document);
        }
        long end = System.currentTimeMillis();
        System.out.println("时间:" + ((end - start) / 1000) + "秒");
    }

    /**
     * 分页查询
     */
    @Test
    public void test5() {
        FindIterable<Document> documents = collection.find();
        //起始下标
        documents.skip(0);
        //每页显示的数据量
        documents.limit(10);
        MongoCursor<Document> iterator = documents.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }

    /**
     * 关闭MongoDB
     */
    @After
    public void end() {
        //5.关闭流
        mongoClient.close();
    }
}
