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

public class CustomerInvoiceUpdater {

    public static void main(String[] args) {
        String csvFilePath = "customer_invoice_mapping.csv";
        String mongoUri = "mongodb://localhost:27017";
        String dbName = "ChinnokNoSQLayer";

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(dbName);

        MongoCollection<Document> customerCollection = database.getCollection("customer");
        MongoCollection<Document> invoiceCollection = database.getCollection("invoice");
        Map<String, List<String>> customerInvoiceMap = new HashMap<>();

        try (
                Reader reader = new InputStreamReader(new FileInputStream(csvFilePath), StandardCharsets.UTF_8);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())
        ) {
            for (CSVRecord record : csvParser) {
                String customerId = record.get("CustomerId");
                String invoiceId = record.get("InvoiceId");
                customerInvoiceMap.computeIfAbsent(customerId, k -> new ArrayList<>()).add(invoiceId);
            }
            for (Map.Entry<String, List<String>> entry : customerInvoiceMap.entrySet()) {
                String customerId = entry.getKey();
                List<String> invoiceIds = entry.getValue();

                Document customerDoc = customerCollection.find(new Document("CustomerId", customerId)).first();

                if (customerDoc != null) {
                    List<String> updatedInvoiceIds = new ArrayList<>();

                    for (String invoiceId : invoiceIds) {
                        Document invoiceDoc = invoiceCollection.find(new Document("InvoiceId", invoiceId)).first();
                        if (invoiceDoc != null) {

                            updatedInvoiceIds.add(invoiceDoc.getString("_id"));
                        } else {
                            System.out.printf("Invoice with InvoiceId %s not found for CustomerId %s%n", invoiceId, customerId);
                        }
                    }
                    customerDoc.remove("invoice");
                    customerDoc.remove("invoices");

                    customerDoc.put("invoice", updatedInvoiceIds);

                    MongoCollection<Document> updatedCustomerCollection = database.getCollection("customer");
                    updatedCustomerCollection.replaceOne(new Document("CustomerId", customerId), customerDoc);

                    Gson gson = new GsonBuilder().setPrettyPrinting().create();
                    try (Writer writer = new FileWriter("updated_customers.json", true)) {
                        gson.toJson(customerDoc, writer);
                    }

                } else {
                    System.out.printf("Customer with CustomerId %s not found.%n", customerId);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            mongoClient.close();
        }
    }
}
