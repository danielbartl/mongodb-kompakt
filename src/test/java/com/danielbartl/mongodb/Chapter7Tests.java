package com.danielbartl.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertOneResult;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.include;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName(
        """
        Praktische Übungsaufgaben aus dem Kapitel 7 des Buches "MongoDB Kompakt"
        """)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Chapter7Tests {

    private static MongoDBContainer mongoDBContainer;
    private static MongoClient mongoClient;
    private static MongoCollection<Document> products;

    @BeforeAll
    static void beforeAll() {

        mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:latest"));

        mongoDBContainer.start();

        mongoClient = MongoClients.create(mongoDBContainer.getConnectionString());

        products =
                mongoClient
                        .getDatabase("onlinemusicshop")
                        .getCollection("produkte");

    }

    @BeforeEach
    void setUp() throws IOException {

        final Document klavier =
                Document.parse(IOUtils.resourceToString("/klavier.json", Charset.defaultCharset()));
        final Document weihnachtsliederbuch =
                Document.parse(IOUtils.resourceToString("/weihnachtsliederbuch.json", Charset.defaultCharset()));
        final Document geige =
                Document.parse(IOUtils.resourceToString("/geige.json", Charset.defaultCharset()));
        final Document stimmgeraet =
                Document.parse(IOUtils.resourceToString("/stimmgeraet.json", Charset.defaultCharset()));
        final Document guitar =
                Document.parse(IOUtils.resourceToString("/guitar.json", Charset.defaultCharset()));
        final Document trompete =
                Document.parse(IOUtils.resourceToString("/trompete.json", Charset.defaultCharset()));

        products.
                insertMany(Arrays.asList(klavier, weihnachtsliederbuch, geige, stimmgeraet, guitar, trompete));

    }

    @Test
    @DisplayName(
            """            
            7.1.1 a) Fügen Sie ein weiteres Produkt Ihrer Wahl ein.
            Halten Sie sich grob an das Schema der bisherigen Produkte.
            """)
    @Order(7111)
    public void testInsertingAdditionalProduct() {

        // given
        assertEquals(6, products.countDocuments());

        // when
        final Document jazzCompilation = new Document("_id", "New York Jazz Lounge");
        jazzCompilation
                .append("kategorie", "Musik")
                .append("preis", new BigDecimal("3000"))
                .append("hersteller", new Document().append("name", "Zip Music").append("land", "USA"))
                .append("schlagworte", List.of("jazz", "CD"));

        final InsertOneResult insertOneResult = products.insertOne(jazzCompilation);

        // then
        assertEquals(
                "New York Jazz Lounge",
                requireNonNull(insertOneResult.getInsertedId()).asString().getValue());
        assertTrue(insertOneResult.wasAcknowledged());

        assertEquals(7, products.countDocuments());

    }

    @Test
    @DisplayName(
            """
            7.1.1 b) Geben Sie alle Produkte des Herstellers "Yomoho" aus,
            die mehr als 100 EUR kosten.
            """)
    @Order(7112)
    void testFindingAllProductsOfYomohoHavingPriceAbove100EUR() {

        // given
        assertEquals(6, products.countDocuments());

        // when
        final FindIterable<Document> result =
                products.find(
                        and(
                                eq("hersteller.name", "Yomoho"),
                                gt("preis", new BigDecimal("100"))
                        )
                );
        final List<Document> list = result.into(new ArrayList<>());

        // then
        assertEquals(2, list.size());
        list.forEach(d -> assertTrue(List.of("Klavier", "Trompete").contains(d.getString("_id"))));


    }

    @Test
    @DisplayName(
            """
            7.1.1 c) Welche Produkte sind in der Kategorie "Zubehoer" oder "Noten"?
            Geben Sie nur deren _id aus.
            """)
    @Order(7113)
    void testFindingProductsOfZubehoerOrNotenCategory() {

        // given
        assertEquals(6, products.countDocuments());

        // when
        final var list =
                products
                        .find(
                                or(eq("kategorie", "Zubehoer"), eq("kategorie", "Noten")))
                        .projection(include("_id"))
                        .into(new ArrayList<>());

        // then
        Assertions.assertEquals(3, list.size());
        list.forEach(d -> assertTrue(List.of("Weihnachtsliederbuch", "Stimmgeraet", "Funky Guitar 5").contains(d.getString("_id"))));

    }

    @Test
    @DisplayName(
            """
            7.1.1 d) Wie viele Produkte haben das Schlagword "jazz"?
            """)
    @Order(7114)
    void testFindingHowManyProductsHaveSchlagwordJazz() {

        // given
        assertEquals(6, products.countDocuments());

        // when
        final long result = products.countDocuments(eq("schlagworte", "jazz"));

        // then
        assertEquals(2, result);

    }

    @AfterEach
    void tearDown() {

        products.drop();

    }

    @AfterAll
    static void afterAll() {

        mongoClient.close();

        mongoDBContainer.stop();

    }
}
