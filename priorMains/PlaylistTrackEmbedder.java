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

public class PlaylistTrackEmbedder {
    public static void main(String[] args) {
        String csvFilePath = "playlist_track.csv";
        String mongoUri = "mongodb://localhost:27017";
        String dbName = "ChinnokAutomaticMapping";

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(dbName);

        MongoCollection<Document> playlistCollection = database.getCollection("playlist");
        MongoCollection<Document> trackCollection = database.getCollection("track");

        Map<String, List<String>> trackToPlaylists = new HashMap<>();
        Map<String, List<String>> playlistToTracks = new HashMap<>();

        try (
                Reader reader = new InputStreamReader(new FileInputStream(csvFilePath), StandardCharsets.UTF_8);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())
        ) {
            for (CSVRecord record : csvParser) {
                String playlistId = record.get("PlaylistId");
                String trackId = record.get("TrackId");

                trackToPlaylists.computeIfAbsent(trackId, k -> new ArrayList<>()).add(playlistId);
                playlistToTracks.computeIfAbsent(playlistId, k -> new ArrayList<>()).add(trackId);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<Document> updatedTracks = new ArrayList<>();
        try (MongoCursor<Document> cursor = trackCollection.find().iterator()) {
            while (cursor.hasNext()) {
                Document track = cursor.next();
                String trackId = track.getString("TrackId");

                List<Document> embeddedPlaylists = new ArrayList<>();
                List<String> playlistIds = trackToPlaylists.getOrDefault(trackId, Collections.emptyList());

                for (String pid : playlistIds) {
                    Document playlist = playlistCollection.find(new Document("PlaylistId", pid)).first();
                    if (playlist != null) {
                        embeddedPlaylists.add(new Document("PlaylistId", pid)
                                .append("Name", playlist.getString("Name")));
                    }
                }

                track.append("playlists", embeddedPlaylists);
                updatedTracks.add(track);
            }
        }

        List<Document> updatedPlaylists = new ArrayList<>();
        try (MongoCursor<Document> cursor = playlistCollection.find().iterator()) {
            while (cursor.hasNext()) {
                Document playlist = cursor.next();
                String playlistId = playlist.getString("PlaylistId");

                List<Document> embeddedTracks = new ArrayList<>();
                List<String> trackIds = playlistToTracks.getOrDefault(playlistId, Collections.emptyList());

                for (String tid : trackIds) {
                    Document track = trackCollection.find(new Document("TrackId", tid)).first();
                    if (track != null) {
                        embeddedTracks.add(new Document("Composer", track.getString("Composer"))
                                .append("Bytes", track.get("Bytes"))
                                .append("MediaTypeId", track.getString("MediaTypeId"))
                                .append("UnitPrice", track.get("UnitPrice"))
                                .append("AlbumId", track.getString("AlbumId"))
                                .append("GenreId", track.getString("GenreId"))
                                .append("Milliseconds", track.get("Milliseconds"))
                                .append("TrackId", tid)
                                .append("Name", track.getString("Name")));
                    }
                }

                playlist.append("tracks", embeddedTracks);
                updatedPlaylists.add(playlist);
            }
        }

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        try (Writer writer1 = new FileWriter("tracks_with_playlists.json");
             Writer writer2 = new FileWriter("playlists_with_tracks.json")) {

            gson.toJson(updatedTracks, writer1);
            gson.toJson(updatedPlaylists, writer2);

            System.out.println("Export complete.");
        } catch (IOException e) {
            e.printStackTrace();
        }

        mongoClient.close();
    }
}
