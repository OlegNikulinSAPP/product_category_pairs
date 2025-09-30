import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col


def setup_spark_environment():
    """Настройка окружения Spark для Windows"""
    # Очистка переменных окружения
    for key in ['PYSPARK_PYTHON', 'PYSPARK_DRIVER_PYTHON', 'HADOOP_HOME']:
        if key in os.environ:
            del os.environ[key]

    # Используем системный Python
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def get_product_category_pairs(
        products: DataFrame,
        categories: DataFrame,
        product_category_links: DataFrame
) -> DataFrame:
    """
    Возвращает все пары «Имя продукта – Имя категории» и имена всех продуктов, у которых нет категорий.

    Args:
        products: Датафрейм с колонками ['product_id', 'product_name']
        categories: Датафрейм с колонками ['category_id', 'category_name']
        product_category_links: Датафрейм с колонками ['product_id', 'category_id']

    Returns:
        Датафрейм с колонками ['product_name', 'category_name']
    """
    # Все пары продукт-категория через левые соединения
    product_categories = (
        products
        .join(product_category_links, "product_id", "left")  # LEFT JOIN чтобы сохранить все продукты
        .join(categories, "category_id", "left")  # LEFT JOIN чтобы добавить названия категорий
        .select(
            col("product_name"),
            col("category_name")
        )
    )

    return product_categories


def main():
    """Основная функция с примером использования"""
    setup_spark_environment()

    try:
        spark = SparkSession.builder.appName("ProductCategoryApp").master("local[1]").getOrCreate()

        # Создаем тестовые данные для продуктов питания
        products_data = [
            (1, "Яблоки"),
            (2, "Молоко"),
            (3, "Хлеб"),  # Без категории
            (4, "Сыр"),   # Несколько категорий
            (5, "Рыба")
        ]

        categories_data = [
            (1, "Фрукты"),
            (2, "Молочные продукты"),
            (3, "Мясо и рыба")
        ]

        links_data = [
            (1, 1),  # Яблоки -> Фрукты
            (2, 2),  # Молоко -> Молочные продукты
            (4, 2),  # Сыр -> Молочные продукты
            (4, 3),  # Сыр -> Мясо и рыба (для примера)
            (5, 3)   # Рыба -> Мясо и рыба
        ]

        # Создаем датафреймы
        products_schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True)
        ])
        products_df = spark.createDataFrame(products_data, products_schema)

        categories_schema = StructType([
            StructField("category_id", IntegerType(), True),
            StructField("category_name", StringType(), True)
        ])
        categories_df = spark.createDataFrame(categories_data, categories_schema)

        links_schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("category_id", IntegerType(), True)
        ])
        links_df = spark.createDataFrame(links_data, links_schema)

        # Показываем исходные данные
        print("Продукты:")
        products_df.show()

        print("Категории:")
        categories_df.show()

        print("Связи продуктов с категориями:")
        links_df.show()

        # Вызываем нашу функцию
        result_df = get_product_category_pairs(products_df, categories_df, links_df)

        # Показываем результат
        print("Результат - пары продукт-категория:")
        result_df.show()

        spark.stop()

    except Exception as e:
        print(f"ОШИБКА: {e}")


if __name__ == "__main__":
    main()
