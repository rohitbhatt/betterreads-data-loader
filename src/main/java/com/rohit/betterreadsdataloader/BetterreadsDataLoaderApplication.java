package com.rohit.betterreadsdataloader;

import com.rohit.betterreadsdataloader.author.Author;
import com.rohit.betterreadsdataloader.author.AuthorService;
import com.rohit.betterreadsdataloader.connection.DataStaxAstraProperties;
import com.rohit.betterreadsdataloader.loader.DataLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.nio.file.Path;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

    @Autowired
    DataLoader dataLoader;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
	}

	@PostConstruct
	public void start(){
	    System.out.println("Application Started");
//        dataLoader.loadAuthors();
//        dataLoader.loadWorks();
    }

    /**
     * This is necessary to have the Spring Boot app use the Astra secure bundle
     * to connect to the database
     */
    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }
}
