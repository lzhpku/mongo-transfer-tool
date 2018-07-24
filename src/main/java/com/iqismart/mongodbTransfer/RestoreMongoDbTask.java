package com.iqismart.mongodbTransfer;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

@Component
@Scope("singleton")
public class RestoreMongoDbTask {

    protected Logger log = LoggerFactory.getLogger(RestoreMongoDbTask.class);

    @Value("${rmt.fromdb.host}")
    private String fromhost ;

    @Value("${rmt.fromdb.port}")
    private Integer fromport ;

    @Value("${rmt.fromdb.database}")
    private String fromdatabase;

    @Value("${rmt.fromdb.username}")
    private String fromusername;

    @Value("${rmt.fromdb.password}")
    private String frompassword;

    @Value("${rmt.todb.host}")
    private String tohost ;

    @Value("${rmt.todb.port}")
    private Integer toport ;

    @Value("${rmt.todb.database}")
    private String todatabase;

    @Value("${rmt.todb.username}")
    private String tousername;

    @Value("${rmt.todb.password}")
    private String topassword;

    @Value("${rmt.collections}")
    private String[] collections;

    @Value("${rmt.oneByOne}")
    private Boolean oneByOne ;

    @Value("${rmt.batchCount}")
    private Integer batchCount ;

    private MongoDatabase mongoDatabaseFrom = null;
    private MongoDatabase mongoDatabaseTo = null;

    public RestoreMongoDbTask() {

    }

    private List<ServerAddress> getServerAddress(String ip, Integer port) {
        List<ServerAddress> serverAddresses = new ArrayList<>();
            serverAddresses.add(new ServerAddress(ip, port));
        return serverAddresses;
    }

    private List<MongoCredential> buildCredential(String username, String password, String database) {
        List<MongoCredential> mongoCredentials = new ArrayList<>();
        mongoCredentials.add(MongoCredential.createScramSha1Credential(username, database, password.toCharArray()));
        return mongoCredentials;
    }

    void start() {
        MongoClientOptions options = MongoClientOptions.builder().connectionsPerHost(4).socketKeepAlive(true).build();
        mongoDatabaseFrom = new MongoClient(getServerAddress(fromhost, fromport), buildCredential(fromusername,
                frompassword, fromdatabase), options).getDatabase(fromdatabase);
        mongoDatabaseTo = new MongoClient(getServerAddress(tohost, toport), buildCredential(tousername,
                topassword, todatabase), options).getDatabase(todatabase);
        log.info("Connect to database successfully");

        for (int i = 0; i < collections.length; i ++) {
            String item = collections[i];
            Long startTIme = System.currentTimeMillis();
            MongoCollection<Document> fromCollection = mongoDatabaseFrom.getCollection(item);
            // MongoCollection<Document> fromCollectionFailed = mongoDatabaseFrom.getCollection(item+"_failed");
            MongoCollection<Document> toCollection = mongoDatabaseTo.getCollection(item);
            log.info("集合" + item + ",选择成功");

            FindIterable<Document> findIterable = fromCollection.find();
            MongoCursor<Document> mongoCursor = findIterable.iterator();
            int successful = 0;
            int failed = 0;

            if (oneByOne) {
                while (mongoCursor.hasNext()) {
                    Document fromDocument = mongoCursor.next();
                    try {
                        toCollection.insertOne(fromDocument);
                        fromCollection.updateOne(Filters.eq("_id", fromDocument.get("_id")), new Document("$set", new Document("transfered", 1)));
                        log.info(fromDocument.get("_id") + " transfer successful , added flag transfered ");
                        successful ++;
                    } catch (Exception e) {
                        log.error(fromDocument.get("_id") + " transfer filed , " + e.getLocalizedMessage());
                    }
                }
                Long endTime = System.currentTimeMillis();
                log.info("集合" + item + ",共处理了" + successful + "个文档,耗时:" + (endTime - startTIme) / 1000 + "s");
            } else {
                List<Document> tempDocumentList = new ArrayList<>();
                while (mongoCursor.hasNext()) {
                    Long startTime = System.currentTimeMillis();
                    Document fromDocument = mongoCursor.next();
                    tempDocumentList.add(fromDocument);
                    try {
                        if (tempDocumentList.size() >= batchCount) {
                            try {
                                toCollection.insertMany(tempDocumentList);
                                successful += tempDocumentList.size();
                                log.info("集合" + item + batchCount + "个文档批量传输成功,use:" + (System.currentTimeMillis() - startTime) + "ms");
                            } catch (Exception e) {
                                // fromCollectionFailed.insertMany(tempDocumentList);
                                failed += tempDocumentList.size();
                                log.error("集合" + item + batchCount + "个文档批量传输失败：" + e.getLocalizedMessage());
                            }
                            tempDocumentList.clear();
                        }
                    } catch (Exception e) {
                        log.error(e.getLocalizedMessage());
                    }
                }

                if (tempDocumentList.size() > 0) {
                    try {
                        Long startTime = System.currentTimeMillis();
                        toCollection.insertMany(tempDocumentList);
                        log.info("集合" + item + tempDocumentList.size() + "个文档批量传输成功,use:" + (System.currentTimeMillis() - startTime) + "ms");
                    } catch (Exception e) {
                        log.error("集合" + item+tempDocumentList.size() + "个零头批量传输失败：" + e.getLocalizedMessage());
                    }
                    successful += tempDocumentList.size();
                    tempDocumentList.clear();
                }

                Long endTime = System.currentTimeMillis();
                log.info("集合" + item + ",共成功了" + successful + "个文档,失败:" + failed + ",use:" + (endTime - startTIme) / 1000 + "s");
            }
        }
    }

}
