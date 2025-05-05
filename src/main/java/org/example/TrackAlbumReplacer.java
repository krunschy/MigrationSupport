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

public class TrackAlbumReplacer {

    public static void main(String[] args) {
        String csvFilePath = "track.csv"; // SQL-exported track CSV
        String outputJsonPath = "track_with_albumId.json";
        String mongoUri = "mongodb://localhost:27017";
        String dbName = "ChinnokAutomaticMapping";

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(dbName);

        MongoCollection<Document> trackCollection = database.getCollection("track");
        MongoCollection<Document> albumCollection = database.getCollection("album");

        try (
                Reader reader = new InputStreamReader(new FileInputStream(csvFilePath), StandardCharsets.UTF_8);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())
        ) {
            Map<String, String> albumIdToMongoId = new HashMap<>();
            List<Document> updatedTracks = new ArrayList<>();

            for (Document album : albumCollection.find()) {
                String albumId = album.getString("AlbumId");
                String mongoId = album.getObjectId("_id").toString();
                if (albumId != null && mongoId != null) {
                    albumIdToMongoId.put(albumId, mongoId);
                }
            }

            for (CSVRecord record : csvParser) {
                String trackId = record.get("TrackId");
                String originalAlbumId = record.get("AlbumId");
                String albumMongoId = albumIdToMongoId.get(originalAlbumId);

                if (albumMongoId == null) {
                    System.out.printf("AlbumId %s not found in MongoDB%n", originalAlbumId);
                    continue;
                }

                Document trackDoc = trackCollection.find(new Document("TrackId", trackId)).first();
                if (trackDoc == null) {
                    System.out.printf("TrackId %s not found in MongoDB%n", trackId);
                    continue;
                }

                trackDoc.put("AlbumId", albumMongoId); // Replace field
                updatedTracks.add(trackDoc);
            }

            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            try (Writer writer = new FileWriter(outputJsonPath)) {
                gson.toJson(updatedTracks, writer);
                System.out.println("Updated track documents written to " + outputJsonPath);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            mongoClient.close();
        }
    }
}
