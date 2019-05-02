package com.bookstore.elasticsearch.dataaccess;

import com.bookstore.elasticsearch.bean.Book;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/***
 * Responsible for CRUD operations to elastic search cluster.
 *
 */
@Repository
public class BookDao {

    private final String INDEX = "bookstore";     // ToDo : we can get this INDEX value form properties file instead hard coding here
    private final String TYPE = "books";          // ToDo : we can get this TYPE value form properties file instead hard coding here

    private RestHighLevelClient restHighLevelClient;

    private ObjectMapper objectMapper;

    public BookDao(ObjectMapper objectMapper, RestHighLevelClient restHighLevelClient) {
        this.objectMapper = objectMapper;
        this.restHighLevelClient = restHighLevelClient;
    }

    //Insert book data to bookstore index 
    public Book insertBook(Book book) {
        book.setId(UUID.randomUUID().toString());
        Map<String, Object> dataMap = objectMapper.convertValue(book, Map.class);
        IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, book.getId()).source(dataMap);
        try {
            restHighLevelClient.index(indexRequest);
        } catch (ElasticsearchException | java.io.IOException e) {
            e.getMessage();
        }
        return book;
    }

    //Get book from bookstore index 
    public Map<String, Object> getBookById(String id) {
        GetRequest getRequest = new GetRequest(INDEX, TYPE, id);
        GetResponse getResponse = null;
        try {
            getResponse = restHighLevelClient.get(getRequest);
        } catch (java.io.IOException e) {
            e.getLocalizedMessage();
        }
        return getResponse.getSourceAsMap();
    }

    //Update book in bookstore index 
    public Map<String, Object> updateBookById(String id, Book book) {
        UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, id).fetchSource(true);

        Map<String, Object> error = new HashMap<>();
        error.put("Error", "Unable to update book in book store");
        try {
            String bookJson = objectMapper.writeValueAsString(book);
            updateRequest.doc(bookJson, XContentType.JSON);
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest);
            return updateResponse.getGetResult().sourceAsMap();
        } catch (java.io.IOException e) {
            e.getMessage();
        }
        return error;
    }

    //Delete book from bookstore index 
    public void deleteBookById(String id) {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
        try {
            restHighLevelClient.delete(deleteRequest);
        } catch (java.io.IOException e) {
            e.getLocalizedMessage();
        }
    }

}
