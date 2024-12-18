package p3;


import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class BookDaoJdbc implements BookDao {
    static Statement st = null;
    static PreparedStatement pst = null;
    static Connection con = null;
    static MysqlDataSource source = null;
    static String name=System.getenv("username");
    static String pass=System.getenv("password");

    static String dbName = "p3_aparanji_database";

    static String url = "jdbc:mysql://p3-aparanji-database.ctosqqsekpjs.us-east-1.rds.amazonaws.com:3306/" + dbName;
    static
    {
        try{
            source = new MysqlDataSource();
            source.setURL(url);
            source.setPassword(pass);
            source.setUser(name);
            con= source.getConnection();
            st = con.createStatement();

        }
        catch(SQLException e){
            System.out.println(e);
        }

    }

    private static final String ADD_BOOK_SQL = "INSERT INTO book (book_id, title, author, description, price, rating, is_public, is_featured, category_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String FIND_ALL_BOOKS_SQL =
            "SELECT book_id, title, author, description, price, rating, is_public, is_featured, category_id " +
                    "FROM book " ;
    private static final String FIND_BY_BOOK_ID_SQL =
            "SELECT book_id, title, author, description, price, rating, is_public, is_featured, category_id " +
                    "FROM book " +
                    "WHERE book_id = ?";

    private static final String FIND_BY_CATEGORY_ID_SQL = "SELECT book_id, title, author, description, price, rating, is_public, is_featured, category_id " +
            "FROM book "+
            "WHERE category_id = ?";

    private static final String FIND_BY_CATEGORY_NAME_SQL = "SELECT book1.book_id, book1.title, book1.author, book1.description, book1.price, book1.rating, book1.is_public, book1.is_featured, book1.category_id "+
            "FROM book book1 JOIN category cat ON book1.category_id = cat.category_id "+
            "WHERE cat.name = ?";
    private static final String FIND_RANDOM_SQL =
            "SELECT book_id, title, author, description, price, rating, is_public, is_featured, category_id  " +
                    "FROM book " +
                    "ORDER BY RAND() " +
                    "LIMIT 5";

    @Override
    public List<Book> findAllBooks() {
        List<Book> books = new ArrayList<>();
        try (
                PreparedStatement statement = con.prepareStatement(FIND_ALL_BOOKS_SQL);
                ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                Book book = readBook(resultSet);
                books.add(book);
            }
        } catch (SQLException e) {
        }
        return books;
    }

    @Override
    public Optional<Book> findByBookId(long bookId) {
        try (
             PreparedStatement statement = con.prepareStatement(FIND_BY_BOOK_ID_SQL)) {
            statement.setLong(1, bookId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                     return Optional.of(readBook(resultSet));
                }
            }
        } catch (SQLException e) {
         }
        return Optional.empty();
    }

    @Override
    public void addBook(long bookId, String title, String author, String description,
                        int price, int rating,
                        boolean isPublic, boolean isFeatured, long categoryId) {
        try (PreparedStatement statement = con.prepareStatement(ADD_BOOK_SQL)) {


            statement.setLong(1,bookId);
            statement.setString(2,title);
            statement.setString(3,author);
            statement.setString(4,description);
            statement.setInt(5,price);
            statement.setInt(6,rating);
            statement.setBoolean(7,isPublic);
            statement.setBoolean(8,isFeatured);
            statement.setLong(9,categoryId);

            int result = statement.executeUpdate();
            //con.commit();
            System.out.println("Rows affected: " + result);
        } catch (SQLException e) {
            System.out.println("Error adding category: " + e.getMessage());
            e.printStackTrace();
        }

    }

    @Override
    public List<Book> findByCategoryId(long categoryId) {
        List<Book> books = new ArrayList<>();
        try (PreparedStatement statement = con.prepareStatement(FIND_BY_CATEGORY_ID_SQL);) {
             statement.setLong(1, categoryId);
             ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Book book = readBook(resultSet);
                books.add(book);
            }
        } catch (SQLException e) {
         }

        return books;
    }

    @Override
    public List<Book> findBookByCategoryName(String categoryName) {
        List<Book> books = new ArrayList<>();
        try (PreparedStatement stmt = con.prepareStatement(FIND_BY_CATEGORY_NAME_SQL)) {
            stmt.setString(1, categoryName);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                books.add(new Book(
                        rs.getLong("book_id"),
                        rs.getString("title"),
                        rs.getString("author"),
                        rs.getString("description"),
                        rs.getInt("price"),
                        rs.getInt("rating"),
                        rs.getBoolean("is_public"),
                        rs.getBoolean("is_featured"),
                        rs.getLong("category_id")
                ));
            }
        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
        }
        return books;
    }

    @Override
    public List<Book> findRandomBooks() {
        List<Book> books = new ArrayList<>();

        try (PreparedStatement statement = con.prepareStatement(FIND_RANDOM_SQL);) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Book book = readBook(resultSet);
                books.add(book);
            }
        } catch (SQLException e) {
        }

        return books;
    }


    private Book readBook(ResultSet resultSet) throws SQLException {
        // TODO add description, isFeatured, rating to Book results
        long bookId = resultSet.getLong("book_id");
        String title = resultSet.getString("title");
        String author = resultSet.getString("author");
        String description = resultSet.getString("description");
        int price = resultSet.getInt("price");
        int rating = resultSet.getInt("rating");
        boolean isPublic = resultSet.getBoolean("is_public");
        boolean isFeatured = resultSet.getBoolean("is_featured");
        long categoryId = resultSet.getLong("category_id");
        return new Book(bookId, title, author, description, price, rating, isPublic, isFeatured, categoryId);
    }
}
