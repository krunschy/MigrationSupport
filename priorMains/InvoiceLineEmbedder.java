package org.example;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.client.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class InvoiceLineEmbedder {
    public static void main(String[] args) {
        String csvFilePath = "invoiceline.csv"; // Ensure this is correct
        String mongoUri = "mongodb://localhost:27017";
        String dbName = "ChinnokAutomaticMapping";

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(dbName);

        MongoCollection<Document> invoiceCollection = database.getCollection("invoice");
        MongoCollection<Document> trackCollection = database.getCollection("track");

        // Lookup tables
        Map<String, List<Document>> invoiceToTracks = new HashMap<>();
        Map<String, List<Document>> trackToInvoices = new HashMap<>();

        try (
                Reader reader = new InputStreamReader(new FileInputStream(csvFilePath), StandardCharsets.UTF_8);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())
        ) {
            for (CSVRecord record : csvParser) {
                String invoiceId = record.get("InvoiceId");
                String trackId = record.get("TrackId");

                Document invoiceDoc = invoiceCollection.find(new Document("InvoiceId", invoiceId)).first();
                Document trackDoc = trackCollection.find(new Document("TrackId", trackId)).first();

                if (invoiceDoc == null || trackDoc == null) {
                    System.out.printf("Missing invoice or track: InvoiceId=%s, TrackId=%s%n", invoiceId, trackId);
                    continue;
                }

                // Prepare track info for embedding into invoice
                Document trackEmbed = new Document()
                        .append("invoiceLineId", record.get("InvoiceLineId"))
                        .append("TrackId", trackDoc.get("TrackId"))
                        .append("Name", trackDoc.get("Name"))
                        .append("Composer", trackDoc.get("Composer"))
                        .append("Bytes", trackDoc.get("Bytes"))
                        .append("MediaTypeId", trackDoc.get("MediaTypeId"))
                        .append("UnitPrice", trackDoc.get("UnitPrice"))
                        .append("AlbumId", trackDoc.get("AlbumId"))
                        .append("GenreId", trackDoc.get("GenreId"))
                        .append("Milliseconds", trackDoc.get("Milliseconds"))
                        .append("UnitPrice", Double.parseDouble(record.get("UnitPrice")))
                        .append("Quantity", Integer.parseInt(record.get("Quantity")));

                invoiceToTracks.computeIfAbsent(invoiceId, k -> new ArrayList<>()).add(trackEmbed);

                // Prepare invoice info for embedding into track
                Document invoiceEmbed = new Document()
                        .append("InvoiceId", invoiceDoc.get("InvoiceId"))
                        .append("InvoiceDate", invoiceDoc.get("InvoiceDate"))
                        .append("BillingAddress", invoiceDoc.get("BillingAddress"))
                        .append("BillingCity", invoiceDoc.get("BillingCity"))
                        .append("BillingCountry", invoiceDoc.get("BillingCountry"))
                        .append("BillingPostalCode", invoiceDoc.get("BillingPostalCode"))
                        .append("Total", invoiceDoc.get("Total"))
                        .append("UnitPrice", Double.parseDouble(record.get("UnitPrice")))
                        .append("Quantity", Integer.parseInt(record.get("Quantity")));

                trackToInvoices.computeIfAbsent(trackId, k -> new ArrayList<>()).add(invoiceEmbed);
            }

            List<Document> updatedInvoices = new ArrayList<>();
            for (Document invoice : invoiceCollection.find()) {
                String id = invoice.getString("InvoiceId");
                invoice.append("invoicelines", invoiceToTracks.getOrDefault(id, new ArrayList<>()));
                updatedInvoices.add(invoice);
            }

            List<Document> updatedTracks = new ArrayList<>();
            for (Document track : trackCollection.find()) {
                String id = track.getString("TrackId");
                track.append("invoicelines", trackToInvoices.getOrDefault(id, new ArrayList<>()));
                updatedTracks.add(track);
            }

            Gson gson = new GsonBuilder().setPrettyPrinting().create();

            try (Writer writer = new FileWriter("invoice_with_lines.json")) {
                gson.toJson(updatedInvoices, writer);
            }
            try (Writer writer = new FileWriter("track_with_invoices.json")) {
                gson.toJson(updatedTracks, writer);
            }

            System.out.println("JSON files created.");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            mongoClient.close();
        }
    }
}
