package com.java.test.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
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
     * 带条件查询一, 通过Lambda表达式遍历数据
     */
    @Test
    public void test6() {
        Document document1 = new Document("$gte", 20);
        Document document2 = new Document("age", document1);
        FindIterable<Document> documents = collection.find(document2);
        MongoCursor<Document> iterator = documents.iterator();
        iterator.forEachRemaining(temp -> System.out.println(temp));
    }

    /**
     * 带条件查询二
     */
    @Test
    public void test7() {
        Document document = Document.parse("{\"age\":{\"$gte\":20}}");
        FindIterable<Document> documents = collection.find(document);
        MongoCursor<Document> iterator = documents.iterator();
        iterator.forEachRemaining(temp -> System.out.println(temp));
    }

    /**
     * 带条件查询三
     */
    @Test
    public void test8() {
        Bson bson = Filters.gte("age", 20);
        FindIterable<Document> documents = collection.find(bson);
        MongoCursor<Document> iterator = documents.iterator();
        iterator.forEachRemaining(temp -> System.out.println(temp));
    }

    /**
     * 带条件查询四
     */
    @Test
    public void test9() {
        Document document = Document.parse("{$and:[{\"age\":{\"$gt\":20}},{\"age\":{\"$lte\":25}}]}");
        FindIterable<Document> documents = collection.find(document);
        MongoCursor<Document> iterator = documents.iterator();
        iterator.forEachRemaining(temp -> System.out.println(temp));
    }

    /**
     * 带条件查询五
     */
    @Test
    public void test10() {
        Bson bson1 = Filters.gt("age", 20);
        Bson bson2 = Filters.lte("age", 25);
        Bson bson = Filters.and(bson1, bson2);
        FindIterable<Document> documents = collection.find(bson);
        MongoCursor<Document> iterator = documents.iterator();
        iterator.forEachRemaining(temp -> System.out.println(temp));
    }

    /**
     * 修改数据
     */
    @Test
    public void test11() {
        Document document1 = Document.parse("{\"gender\":\"女\"}");
        Document document2 = Document.parse("{$set:{\"name\":\"test\",\"age\":30}}");
        UpdateResult updateResult = collection.updateMany(document1, document2);
        System.out.println("updateResult=" + updateResult);
    }

    /**
     * 删除数据
     */
    @Test
    public void test12() {
        Document document1 = Document.parse("{\"gender\":\"女\"}");
        DeleteResult deleteResult = collection.deleteMany(document1);
        System.out.println("deleteResult=" + deleteResult);
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
