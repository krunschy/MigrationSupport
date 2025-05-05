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
        String csvFilePath = "customer_invoice_mapping.csv"; // Path to the CSV mapping file
        String mongoUri = "mongodb://localhost:27017"; // MongoDB URI
        String dbName = "ChinnokNoSQLayer"; // Database name

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(dbName);

        MongoCollection<Document> customerCollection = database.getCollection("customer");
        MongoCollection<Document> invoiceCollection = database.getCollection("invoice");

        // This will hold the mapping from CustomerId to a list of new invoice _id's
        Map<String, List<String>> customerInvoiceMap = new HashMap<>();

        try (
                Reader reader = new InputStreamReader(new FileInputStream(csvFilePath), StandardCharsets.UTF_8);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())
        ) {
            // Read the CSV and build the map
            for (CSVRecord record : csvParser) {
                String customerId = record.get("CustomerId");
                String invoiceId = record.get("InvoiceId");

                // Add invoiceId to the list of invoices for the given customerId
                customerInvoiceMap.computeIfAbsent(customerId, k -> new ArrayList<>()).add(invoiceId);
            }

            // Update each customer document
            for (Map.Entry<String, List<String>> entry : customerInvoiceMap.entrySet()) {
                String customerId = entry.getKey();
                List<String> invoiceIds = entry.getValue();

                // Find the customer document by CustomerId
                Document customerDoc = customerCollection.find(new Document("CustomerId", customerId)).first();

                if (customerDoc != null) {
                    List<String> updatedInvoiceIds = new ArrayList<>();

                    // For each InvoiceId, find the corresponding _id in the invoice collection
                    for (String invoiceId : invoiceIds) {
                        Document invoiceDoc = invoiceCollection.find(new Document("InvoiceId", invoiceId)).first();
                        if (invoiceDoc != null) {
                            // Add the invoice _id as a string to the updated list
                            updatedInvoiceIds.add(invoiceDoc.getString("_id"));
                        } else {
                            System.out.printf("Invoice with InvoiceId %s not found for CustomerId %s%n", invoiceId, customerId);
                        }
                    }
                    customerDoc.remove("invoice");
                    customerDoc.remove("invoices");

                    // Set the newInvoices array in the customer document with the updated invoice _ids
                    customerDoc.put("invoice", updatedInvoiceIds);

                    // Optionally, write the updated customer document to a JSON file
                    // Or, update it directly in MongoDB
                    MongoCollection<Document> updatedCustomerCollection = database.getCollection("customer");
                    updatedCustomerCollection.replaceOne(new Document("CustomerId", customerId), customerDoc);

                    // Print to JSON (for debugging or later use)
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
