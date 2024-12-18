
package p3;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import org.json.JSONObject;
import com.google.gson.Gson;


import java.util.List;
import java.util.Map;

public class dbLambdaHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private CategoryDaoJdbc categoryDb = new CategoryDaoJdbc();
    private BookDaoJdbc bookDb = new BookDaoJdbc();

    Gson gson = new Gson();
    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent req, Context context) {
        Map<String, String> CORS = Map.of("access-control-allow-origin", "*");
        String path = req.getPath();
        System.out.println("Received path: " + path);
        if (path.equals("/api/getAllCategory")) {
            return getAllCategories(CORS);
        } else if (path.equals("/api/getCategoryName")) {
            return getCategoryName(req, CORS);
        } else if (path.equals("/api/getCategoryId")) {
            return getCategoryId(req, CORS);
        } else if (path.equals("/api/addCategory")) {
            return addCategory(req, CORS);
        } else if (path.equals("/api/getAllBook")) {
            return getAllBook(CORS);
        } else if (path.equals("/api/addBook")) {
            return addNewBook(req, CORS);
        } else if (path.equals("/api/getBookById")) {
            return getBookById(req, CORS);
        } else if (path.equals("/api/getBookByCategoryId")) {
            return getBookByCategoryId(req, CORS);
        } else if (path.equals("/api/getBookByCategoryName")) {
            return getBookByCategoryName(req, CORS);
        } else if (path.equals("/api/getRandomBook")) {
            return getRandomBooks(CORS);
        } else {
            return new APIGatewayProxyResponseEvent()
                    .withBody("Invalid request")
                    .withHeaders(CORS)
                    .withStatusCode(404);
        }

    }

    private APIGatewayProxyResponseEvent getAllCategories(Map<String, String> CORS) {
        List<Category> categories = categoryDb.findAllCategories();

        String json = gson.toJson(categories);

        return new APIGatewayProxyResponseEvent()
                .withBody(json)
                .withHeaders(CORS)
                .withStatusCode(200);
    }


    private APIGatewayProxyResponseEvent getCategoryName(APIGatewayProxyRequestEvent req, Map<String, String> CORS) {
        long categoryId = Long.parseLong(req.getQueryStringParameters().get("categoryId"));
        Category category = categoryDb.findByCategoryId(categoryId);
        String json = gson.toJson(Map.of("name", category.name()));
        return new APIGatewayProxyResponseEvent()
                .withBody(json)
                .withHeaders(CORS)
                .withStatusCode(200);
    }

    private APIGatewayProxyResponseEvent getCategoryId(APIGatewayProxyRequestEvent req, Map<String, String> CORS) {
        String categoryName = req.getQueryStringParameters().get("name");
        Category category = categoryDb.findByName(categoryName);
        String json = gson.toJson(Map.of("categoryId", category.categoryId()));
        return new APIGatewayProxyResponseEvent()
                .withBody(json)
                .withHeaders(CORS)
                .withStatusCode(200);
    }

    private APIGatewayProxyResponseEvent addCategory(APIGatewayProxyRequestEvent req, Map<String, String> CORS) {
        String addedCategory = req.getBody();

        JSONObject obj = new JSONObject(addedCategory);
        String categoryName = obj.getString("name");
        long categoryId = obj.getLong("categoryId");


        Category newCategory = new Category(categoryId, categoryName);
        categoryDb.addCategory(newCategory.categoryId(), newCategory.name());
        String json = gson.toJson(Map.of("message", "Category added: " + newCategory.name()));


        return new APIGatewayProxyResponseEvent()
                .withBody(json)
                .withHeaders(CORS)
                .withStatusCode(201);
    }

    private APIGatewayProxyResponseEvent getAllBook(Map<String, String> CORS) {
        List<Book> books = bookDb.findAllBooks();
        String json = gson.toJson(books);
        return new APIGatewayProxyResponseEvent()
                .withBody(json)
                .withHeaders(CORS)
                .withStatusCode(200);
    }

    private APIGatewayProxyResponseEvent addNewBook(APIGatewayProxyRequestEvent req, Map<String, String> CORS) {
        String addedBook = req.getBody();
        JSONObject obj = new JSONObject(addedBook);

        long bookId = obj.getLong("bookId");
        String bookTitle =  obj.getString("title");
        String bookAuthor =   obj.getString("author");
        String bookDesc =   obj.getString("description");
        int bookPrice =  obj.getInt("price");
        int bookRating =  obj.getInt("rating");
        boolean bookPublic =   obj.getBoolean("isPublic");
        boolean bookFeatured =   obj.getBoolean("isFeatured");
        long CategoryId =   obj.getLong("categoryId");

        Book newBook= new Book(bookId, bookTitle, bookAuthor,bookDesc,bookPrice,bookRating,bookPublic,bookFeatured,CategoryId);

        bookDb.addBook(
                newBook.bookId(),
                newBook.title(),
                newBook.author(),
                newBook.description(),
                newBook.price(),
                newBook.rating(),
                newBook.isPublic(),
                newBook.isFeatured(),
                newBook.categoryId());

        String json = gson.toJson(Map.of("message", "Book added: " + newBook.title()));
        return new APIGatewayProxyResponseEvent()
                .withBody(json)
                .withHeaders(CORS)
                .withStatusCode(201);
    }

    private APIGatewayProxyResponseEvent getBookById(APIGatewayProxyRequestEvent req, Map<String, String> CORS) {
        long bookId = Long.parseLong(req.getQueryStringParameters().get("bookId"));


        return bookDb.findByBookId(bookId)
                .map(book -> {
                    String json = gson.toJson(book);
                    return new APIGatewayProxyResponseEvent()
                            .withBody(json)
                            .withHeaders(CORS)
                            .withStatusCode(200);
                })
                .orElseGet(() -> new APIGatewayProxyResponseEvent()
                        .withBody("{\"message\":\"Book not found\"}")
                        .withHeaders(CORS)
                        .withStatusCode(404));
    }

    private APIGatewayProxyResponseEvent getBookByCategoryId(APIGatewayProxyRequestEvent req, Map<String, String> CORS) {
        long categoryId = Long.parseLong(req.getQueryStringParameters().get("categoryId"));
        List<Book> books = bookDb.findByCategoryId(categoryId);
        String json = gson.toJson(books);
        return new APIGatewayProxyResponseEvent()
                .withBody(json)
                .withHeaders(CORS)
                .withStatusCode(200);
    }

    private APIGatewayProxyResponseEvent getBookByCategoryName(APIGatewayProxyRequestEvent req, Map<String, String> CORS) {
        String categoryName = req.getQueryStringParameters().get("name");
        List<Book> books = bookDb.findBookByCategoryName(categoryName);
        String json = gson.toJson(books);

        return new APIGatewayProxyResponseEvent()
                .withBody(json)
                .withHeaders(CORS)
                .withStatusCode(200);
    }

    private APIGatewayProxyResponseEvent getRandomBooks(Map<String, String> CORS) {
        List<Book> books = bookDb.findRandomBooks();
        if (books.isEmpty()) {
            return new APIGatewayProxyResponseEvent()
                    .withBody("No books available")
                    .withHeaders(CORS)
                    .withStatusCode(404);
        } else {
            StringBuilder sb = new StringBuilder();
            for (Book book : books) {
                sb.append(book.toString()).append("\n");
            }
            return new APIGatewayProxyResponseEvent()
                    .withBody(sb.toString())
                    .withHeaders(CORS)
                    .withStatusCode(200);
        }
    }

}
