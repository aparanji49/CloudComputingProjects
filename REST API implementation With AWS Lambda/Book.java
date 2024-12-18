package p3;


public record Book(long bookId, String title, String author, String description,
				   int price, int rating,
				   boolean isPublic, boolean isFeatured, long categoryId) {}
