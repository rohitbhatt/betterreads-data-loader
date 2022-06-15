package com.rohit.betterreadsdataloader.book;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BookService {
    @Autowired
    BookRepository bookRepository ;

    public Book save(Book book){
        return bookRepository.save(book);
    }
}


