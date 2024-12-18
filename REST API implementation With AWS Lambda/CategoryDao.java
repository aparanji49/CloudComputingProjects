package p3;

import java.util.List;

public interface CategoryDao {

    public List<Category> findAllCategories();

    public void addCategory(long categoryId, String name);
    public Category findByCategoryId(long categoryId);

    public Category findByName(String categoryName);
}
