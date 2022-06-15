package com.rohit.betterreadsdataloader.loader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rohit.betterreadsdataloader.author.Author;
import com.rohit.betterreadsdataloader.author.AuthorService;
import com.rohit.betterreadsdataloader.book.Book;
import com.rohit.betterreadsdataloader.book.BookService;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class DataLoader {

    @Value("${datadump.location.authordump}")
    private String authorsdumpLocation;

    @Value("${datadump.location.worksdump}")
    private String worksdumpLocation;

    @Autowired
    AuthorService authorService;

    @Autowired
    BookService bookService;

    public void loadAuthors(){
        AtomicInteger counter = new AtomicInteger();
        final Path path = Paths.get(authorsdumpLocation);
        try (Stream<String> lines = Files.lines(path)) {
            lines.forEach(line->{
                String authorJson = line.substring(line.indexOf("{"));

                try {
                    JSONObject authorObject =  new JSONObject(authorJson);
                    Author author = new Author(authorObject.optString("key").replace("/authors/",""), authorObject.optString("name"), authorObject.optString("personal_name"));
                    System.out.println(author);
                    authorService.save(author);
                    counter.getAndIncrement();

                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        System.out.println("Loaded Authors Count: "+ counter);
    }

    public void loadWorks(){
        AtomicInteger counter = new AtomicInteger();
        final Path path = Paths.get(worksdumpLocation);
        try (Stream<String> lines = Files.lines(path)) {
            lines.forEach(line->{
                // Read and Parse the line
                String workJson = line.substring(line.indexOf("{"));

                try {
                    JSONObject workObject =  new JSONObject(workJson);
                    // Construct Book Object
                    Book book = new Book();

                    book.setId(workObject.getString("key").replace("/works/",""));

                    book.setName(workObject.optString("title"));

                    JSONObject descriptionObject = workObject.optJSONObject("description");
                    if(descriptionObject!=null){
                        book.setDescription(descriptionObject.optString("value"));
                    }

                    JSONObject publishedDateObject = workObject.optJSONObject("created");
                    if(publishedDateObject!=null){
                        String dateStr = publishedDateObject.optString("value");
                        book.setPublishedDate(LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                    }

                    JSONArray coversJsonArray = workObject.optJSONArray("covers");
                    if(coversJsonArray!=null){
                        List<String> coverIds = new ArrayList<>();
                        for (int i=0; i < coversJsonArray.length(); i++){
                            coverIds.add( coversJsonArray.getString(i) );
                        }
                        book.setCoverIds(coverIds);
                    }

                    JSONArray authorsJSONArray = workObject.optJSONArray("authors");
                    if (authorsJSONArray!=null)
                    {
                        List<String> authorIds = new ArrayList<>();
                        for(int i=0; i < authorsJSONArray.length(); i++){
                            String authorId = authorsJSONArray.getJSONObject(i).getJSONObject("author").getString("key").replace("/authors/","");
                            authorIds.add(authorId);
                        }
                        book.setAuthorIds(authorIds);

                        List<String> authorNames = authorIds
                                .stream()
                                .map(id -> authorService.findById(id))
                                .map(optionalAuthor -> {
                                    if (optionalAuthor.isPresent())
                                        return optionalAuthor.get().getName();
                                    else
                                        return "Unknown Author";
                                }).collect(Collectors.toList());

                        book.setAuthorNames(authorNames);
                        bookService.save(book);
                    }

                    counter.getAndIncrement();
                    System.out.println(book);

                } catch (JSONException e) {
                    System.err.println("Error Catch1");
                    e.printStackTrace();
                }
            });
        } catch (IOException ex) {
            System.err.println("Error Catch2");
            ex.printStackTrace();
        }
        System.out.println("Loaded Works Count: "+ counter);
    }
}
