package co.uk.mongodb.renfer.sem;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.FindIterable;

import org.bson.Document;
import org.bson.RawBsonDocument;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;

import static com.mongodb.client.model.Filters.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SEMArchivingTest {

    private MongoClient mongoClient;
    private String dealId  = "IzzBMFw_EemtfZP7oMavCgA";
    private String tlgId = "vrBLTfu8RFKzK0S8+TodOw==";

    public SEMArchivingTest(){
        var connectionString = "mongodb://localhost/?w=majority";
        mongoClient = MongoClients.create(connectionString);
    }

    @BeforeAll
    public void initDatabase(){
        MongoDatabase sem = mongoClient.getDatabase("sem");

        mongoClient.getDatabase("sem").drop();
        mongoClient.getDatabase("archive").drop();

        MongoCollection<Document> deals = sem.getCollection("deal");
        MongoCollection<Document> belvs = sem.getCollection("belv");
        MongoCollection<Document> taxlotEffects = sem.getCollection("taxloteffect");
        MongoCollection<Document> accrualEffects = sem.getCollection("accrualEffect");
        MongoCollection<Document> taxlots = sem.getCollection("taxlot");

        deals.insertOne(new Document().append("_id", new Document().append("id", dealId).append("version", 1)));
        deals.insertOne(new Document().append("_id", new Document().append("id", dealId).append("version", 2)));
        deals.insertOne(new Document().append("_id", new Document().append("id", dealId).append("version", 3)));

        belvs.insertOne(
                new Document().append("BusinessEvent",
                        new Document().append("deal",
                                new Document().append("dealId", dealId)
                        ).append("taxLotGroupId", tlgId)
                ).append("meta", new Document().append("txId", "12345678"))
        );
        belvs.insertOne(
                new Document().append("BusinessEvent",
                        new Document().append("deal",
                                new Document().append("dealId", dealId)
                        ).append("taxLotGroupId", tlgId)
                ).append("meta", new Document().append("txId", "87654321"))
        );

        taxlotEffects.insertOne(
                new Document("key",
                        new Document("txId", "12345678")
                ).append("data", new Document("tls", Arrays.asList(1, 2)))
        );

        taxlotEffects.insertOne(
                new Document("key",
                        new Document("txId", "87654321")
                ).append("data", new Document("tls", Arrays.asList(3, 4)))
        );

        taxlots.insertOne(new Document("_id", 1));
        taxlots.insertOne(new Document("_id", 2));
        taxlots.insertOne(new Document("_id", 3));
        taxlots.insertOne(new Document("_id", 4));

        accrualEffects.insertOne(
                new Document("key",
                        new Document("txId", "12345678")
                ).append("data", new Document("tls", Arrays.asList(1, 2)))
        );

        accrualEffects.insertOne(
                new Document("key",
                        new Document("txId", "87654321")
                ).append("data", new Document("tls", Arrays.asList(3, 4)))
        );
    }

    @Test
    void archiveSingleDeal() {
        MongoDatabase sem = mongoClient.getDatabase("sem");
        MongoDatabase archive = mongoClient.getDatabase("archive");

        MongoCollection<RawBsonDocument> deals = sem.getCollection("deal", RawBsonDocument.class);
        MongoCollection<RawBsonDocument> deals_archive = archive.getCollection("deal", RawBsonDocument.class);

        MongoCollection<RawBsonDocument> belvs = sem.getCollection("belv", RawBsonDocument.class);
        MongoCollection<RawBsonDocument> belvs_archive = archive.getCollection("belv", RawBsonDocument.class);

        MongoCollection<RawBsonDocument> taxlotEffects = sem.getCollection("taxloteffect", RawBsonDocument.class);
        MongoCollection<RawBsonDocument> taxlotEffects_archive = archive.getCollection("taxloteffect", RawBsonDocument.class);

        MongoCollection<RawBsonDocument> accrualEffects = sem.getCollection("accrualEffect", RawBsonDocument.class);
        MongoCollection<RawBsonDocument> accrualEffects_archive = archive.getCollection("accrualEffect", RawBsonDocument.class);

        MongoCollection<RawBsonDocument> taxlots = sem.getCollection("taxlot", RawBsonDocument.class);
        MongoCollection<RawBsonDocument> taxlots_archive = archive.getCollection("taxlot", RawBsonDocument.class);

        // archive all deals in archive
        final FindIterable<RawBsonDocument> rawBsonDealsIterable = deals.find(eq("_id.id", dealId));

        // archive each deal separately
        rawBsonDealsIterable.forEach(deals_archive::insertOne);

        // load all deals into memory and archive them with insertMany
        //deals_archive.insertMany(StreamSupport.stream(rawBsonDealsIterable.spliterator(), false).collect(Collectors.toList()));


        // get belvs for deal
        belvs.find(and(eq("BusinessEvent.deal.dealId", dealId), eq("BusinessEvent.taxLotGroupId", tlgId))).forEach(belv -> {

            // archive all belvs in archive
            belvs_archive.insertOne(belv);

            // get taxlotEffectId
            String txId = belv.getDocument("meta").get("txId").asString().getValue();
            //String txId = belv.getString("meta.txId").getValue();

            //get all taxlotEffects
            taxlotEffects.find(eq("key.txId", txId)).forEach(taxlotEffect -> {

                // archive taxlotEfects
                taxlotEffects_archive.insertOne(taxlotEffect);

                // archive taxLots
                taxlots.find(in("_id", taxlotEffect.getDocument("data").getArray("tls").toArray())).forEach(taxlots_archive::insertOne);
            });

            // archive all accrualEffects
            accrualEffects.find(eq("key.txId", txId)).forEach(accrualEffects_archive::insertOne);
        });
    }

    @AfterAll
    void tearDown(){
        mongoClient.close();
    }
}