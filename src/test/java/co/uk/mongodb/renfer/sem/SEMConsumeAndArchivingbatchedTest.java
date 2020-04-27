package co.uk.mongodb.renfer.sem;

import com.mongodb.client.*;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SEMConsumeAndArchivingbatchedTest {

    private MongoClient mongoClient;
    private String dealId  = "IzzBMFw_EemtfZP7oMavCgA";
    private String tlgId = "vrBLTfu8RFKzK0S8+TodOw==";

    public SEMConsumeAndArchivingbatchedTest(){
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
        taxlots.insertOne(new Document("_id", 5));
        taxlots.insertOne(new Document("_id", 6));
        taxlots.insertOne(new Document("_id", 7));
        taxlots.insertOne(new Document("_id", 8));

        accrualEffects.insertOne(
                new Document("key",
                        new Document("txId", "12345678")
                ).append("data", new Document("accs", Arrays.asList(new Document("tl", 5), new Document("tl", 6))))
        );

        accrualEffects.insertOne(
                new Document("key",
                        new Document("txId", "87654321")
                ).append("data", new Document("accs", Arrays.asList(new Document("tl", 7), new Document("tl", 8))))
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

        // define batch sizes
        int dealsBatchSize = 100;
        int belvsBatchSize = 100;
        int taxlotEffectsBatchSize = 100;
        int taxlotsBatchSize = 100;
        int accrualEffectsBatchSize = 100;

        // archive all deals in archive
        archiveBatched(deals_archive, deals.find(eq("_id.id", dealId)), dealsBatchSize);

        consumeAndArchiveBatched(belvs_archive, belvs.find(and(eq("BusinessEvent.deal.dealId", dealId), eq("BusinessEvent.taxLotGroupId", tlgId))), belvsBatchSize, belv -> {
            // get taxlotEffectId
            String txId = belv.getDocument("meta").get("txId").asString().getValue();
            //String txId = belv.getString("meta.txId").getValue();

            //get all taxlotEffects
            consumeAndArchiveBatched(taxlotEffects_archive, taxlotEffects.find(eq("key.txId", txId)), taxlotEffectsBatchSize, taxlotEffect -> {
                archiveBatched(taxlots_archive, taxlots.find(in("_id", taxlotEffect.getDocument("data").getArray("tls").toArray())) ,taxlotsBatchSize);
            });

            // archive all accrualEffects
            consumeAndArchiveBatched(accrualEffects_archive, accrualEffects.find(eq("key.txId", txId)),accrualEffectsBatchSize, doc -> {
                final Object[] taxlotIds =
                        doc.getDocument("data")
                                .getArray("accs")
                                .getValues()
                                .stream()
                                .map(acc -> acc.asDocument().get("tl"))
                                .toArray();

                archiveBatched(taxlots_archive,taxlots.find(in("_id", taxlotIds)), taxlotsBatchSize);
            } );
        });
    }

    private void consumeAndArchiveBatched(MongoCollection<RawBsonDocument> archive, FindIterable<RawBsonDocument> resultIterable, int batchSize, Consumer<RawBsonDocument> consumer) {
        final MongoCursor<RawBsonDocument> cursor = resultIterable.batchSize(batchSize).cursor();
        while (cursor.hasNext()) {
            final List<RawBsonDocument> rawBsonDocuments = consumeBatch(cursor, batchSize, consumer);
            archive.insertMany(rawBsonDocuments);
        }
    }

    private List<RawBsonDocument> consumeBatch(MongoCursor<RawBsonDocument> cursor, int batchSize, Consumer<RawBsonDocument> consumer) {
        ArrayList<RawBsonDocument> batchOfDocs = new ArrayList<>();
        for (int i = 0; i < batchSize && cursor.hasNext(); i++) {
            final RawBsonDocument next = cursor.next();
            consumer.accept(next);
            batchOfDocs.add(next);
        }
        return batchOfDocs;
    }

    private void archiveBatched(MongoCollection<RawBsonDocument> archive, FindIterable<RawBsonDocument> resultIterable, int batchSize) {
        final MongoCursor<RawBsonDocument> cursor = resultIterable.batchSize(batchSize).cursor();
        while (cursor.hasNext()) {
            final List<RawBsonDocument> rawBsonDocuments = nextBatch(cursor, batchSize);
            archive.insertMany(rawBsonDocuments);
        }
    }

    private List<RawBsonDocument> nextBatch(MongoCursor<RawBsonDocument> cursor, int batchSize) {
        ArrayList<RawBsonDocument> batchOfDocs = new ArrayList<>();
        for (int i = 0; i < batchSize && cursor.hasNext(); i++) {
            batchOfDocs.add(cursor.next());
        }
        return batchOfDocs;
    }

    @AfterAll
    void tearDown(){
        mongoClient.close();
    }
}