package p3;

import java.util.List;
import java.util.Optional;

public interface BookDao {

    public List<Book> findAllBooks();
    public Optional<Book> findByBookId(long bookId);

    public void addBook(long bookId, String title, String author, String description,
                        int price, int rating,
                        boolean isPublic, boolean isFeatured, long categoryId);
    public List<Book> findByCategoryId(long categoryId);
    public List<Book> findBookByCategoryName(String categoryName);

    public List<Book> findRandomBooks();

}

