import os
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import lit

# Указываем путь к Java
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-24"


def get_product_category_pairs(
        products_df: DataFrame,
        categories_df: DataFrame,
        relations_df: DataFrame
) -> DataFrame:
    product_category = (
        relations_df.join(products_df, "product_id", "inner")
        .join(categories_df, "category_id", "inner")
        .select("product_name", "category_name")
    )
    no_category = (
        products_df.join(relations_df, "product_id", "left_anti")
        .select("product_name")
        .withColumn("category_name", lit(None)))
    return product_category.union(no_category)


def main():
    spark = SparkSession.builder \
        .appName("ProductCategoryTest") \
        .master("local") \
        .getOrCreate()

    try:
        # Тестовые данные
        products = spark.createDataFrame([
            Row(product_id=1, product_name="Телефон"),
            Row(product_id=2, product_name="Ноутбук")
        ])

        categories = spark.createDataFrame([
            Row(category_id=1, category_name="Электроника")
        ])

        relations = spark.createDataFrame([
            Row(product_id=1, category_id=1)
        ])

        result = get_product_category_pairs(products, categories, relations)
        result.show()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()