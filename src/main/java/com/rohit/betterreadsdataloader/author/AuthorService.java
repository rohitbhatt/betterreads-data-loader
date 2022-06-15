package com.rohit.betterreadsdataloader.author;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class AuthorService {
    @Autowired
    AuthorRepository authorRepository;

    public Author save(Author author){
        return authorRepository.save(author);
    }

    public Optional<Author> findById(String authorId){
        return authorRepository.findById(authorId);
    }
}


