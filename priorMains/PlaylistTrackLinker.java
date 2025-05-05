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

public class PlaylistTrackLinker {
    public static void main(String[] args) {
        String csvFilePath = "playlist_track.csv";
        String mongoUri = "mongodb://localhost:27017";
        String dbName = "ChinnokAutomaticMapping";

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(dbName);

        MongoCollection<Document> trackCollection = database.getCollection("track");
        MongoCollection<Document> playlistCollection = database.getCollection("playlist");

        List<Document> linkingDocs = new ArrayList<>();

        try (
                Reader reader = new InputStreamReader(new FileInputStream(csvFilePath), StandardCharsets.UTF_8);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())
        ) {
            for (CSVRecord record : csvParser) {
                String playlistId = record.get("PlaylistId");
                String trackId = record.get("TrackId");

                Document playlistDoc = playlistCollection.find(new Document("PlaylistId", playlistId)).first();
                Document trackDoc = trackCollection.find(new Document("TrackId", trackId)).first();

                if (playlistDoc != null && trackDoc != null) {
                    String playlistMongoId = playlistDoc.getString("_id");
                    String trackMongoId = trackDoc.getString("_id");

                    Document link = new Document("playlistId", playlistMongoId)
                            .append("trackId", trackMongoId);

                    linkingDocs.add(link);
                } else {
                    System.out.printf("Missing PlaylistId or TrackId: %s, %s%n", playlistId, trackId);
                }
            }

            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            try (Writer writer = new FileWriter("playlist_track_links.json")) {
                gson.toJson(linkingDocs, writer);
            }

            System.out.println("Linking JSON file created successfully!");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            mongoClient.close();
        }
    }
}
