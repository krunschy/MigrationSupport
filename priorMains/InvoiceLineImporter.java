package org.example;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class InvoiceImporter {
    public static void main(String[] args) {
        String csvFilePath = "invoice.csv"; // Place your CSV file here or update the path
        String outputJsonPath = "invoice_cleaned.json";

        List<Document> invoiceDocs = new ArrayList<>();

        try (
                Reader reader = new InputStreamReader(new FileInputStream(csvFilePath), StandardCharsets.UTF_8);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())
        ) {
            for (CSVRecord record : csvParser) {
                String invoiceId = record.get("InvoiceId");
                // Skipping CustomerId
                String invoiceDate = record.get("InvoiceDate");
                String billingAddress = record.get("BillingAddress");
                String billingCity = record.get("BillingCity");
                String billingState = record.get("BillingState");
                String billingCountry = record.get("BillingCountry");
                String billingPostalCode = record.get("BillingPostalCode");
                String total = record.get("Total");

                Document invoiceDoc = new Document("_id", invoiceId)  // Keeping InvoiceId as _id
                        .append("invoiceDate", invoiceDate)
                        .append("billingAddress", billingAddress)
                        .append("billingCity", billingCity)
                        .append("billingState", billingState)
                        .append("billingCountry", billingCountry)
                        .append("billingPostalCode", billingPostalCode)
                        .append("total", Double.parseDouble(total));

                invoiceDocs.add(invoiceDoc);
            }

            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            try (Writer writer = new FileWriter(outputJsonPath)) {
                gson.toJson(invoiceDocs, writer);
            }

            System.out.println("Invoice documents successfully written to " + outputJsonPath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
