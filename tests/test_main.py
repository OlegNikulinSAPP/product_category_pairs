import unittest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession

from main import setup_spark_environment, get_product_category_pairs


class TestProductCategoryPairs(unittest.TestCase):
    """Тесты для функции get_product_category_pairs"""

    spark = None  # Явно объявляем атрибут для статического анализатора

    @classmethod
    def setUpClass(cls):
        """Настройка Spark перед всеми тестами"""
        setup_spark_environment()
        cls.spark = SparkSession.builder.appName("TestProductCategoryApp").master("local[1]").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        """Очистка после всех тестов"""
        if cls.spark:
            cls.spark.stop()

    def create_test_dataframes(self, products_data, categories_data, links_data):
        """Создание тестовых датафреймов из данных"""
        # Проверяем, что spark инициализирован
        if not self.spark:
            self.fail("Spark session not initialized")

        products_schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True)
        ])
        products_df = self.spark.createDataFrame(products_data, products_schema)

        categories_schema = StructType([
            StructField("category_id", IntegerType(), True),
            StructField("category_name", StringType(), True)
        ])
        categories_df = self.spark.createDataFrame(categories_data, categories_schema)

        links_schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("category_id", IntegerType(), True)
        ])
        links_df = self.spark.createDataFrame(links_data, links_schema)

        return products_df, categories_df, links_df

    def assert_dataframes_equal(self, result_df, expected_data):
        """Проверка, что датафрейм содержит ожидаемые данные"""
        result_data = []
        for row in result_df.collect():
            # Обрабатываем None значения
            category_name = row['category_name'] if row['category_name'] is not None else None
            result_data.append((row['product_name'], category_name))

        result_data_sorted = sorted(result_data)
        expected_data_sorted = sorted(expected_data)

        self.assertEqual(result_data_sorted, expected_data_sorted)

    def test_all_products_have_categories(self):
        """Тест 1: Все продукты имеют категории"""
        products_data = [
            (1, "Яблоки"),
            (2, "Молоко"),
            (3, "Хлеб")
        ]
        categories_data = [
            (1, "Фрукты"),
            (2, "Молочные продукты")
        ]
        links_data = [
            (1, 1),
            (2, 2),
            (3, 2)  # для теста
        ]

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        expected_result = [
            ("Яблоки", "Фрукты"),
            ("Молоко", "Молочные продукты"),
            ("Хлеб", "Молочные продукты")  # для теста
        ]

        result_df = get_product_category_pairs(products_df, categories_df, links_df)
        self.assert_dataframes_equal(result_df, expected_result)

    def test_products_without_categories(self):
        """Тест 2: Есть продукты без категорий"""
        products_data = [
            (1, "Яблоки"),
            (2, "Молоко"),
            (3, "Хлеб"),
            (4, "Сыр")
        ]
        categories_data = [
            (1, "Фрукты"),
            (2, "Молочные продукты")
        ]
        links_data = [
            (1, 1),
            (2, 2)
        ]

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        expected_result = [
            ("Яблоки", "Фрукты"),
            ("Молоко", "Молочные продукты"),
            ("Хлеб", None),
            ("Сыр", None)
        ]

        result_df = get_product_category_pairs(products_df, categories_df, links_df)
        self.assert_dataframes_equal(result_df, expected_result)

    def test_products_with_multiple_categories(self):
        """Тест 3: Продукты с несколькими категориями"""
        products_data = [
            (1, "Сыр"),
            (2, "Рыба")
        ]
        categories_data = [
            (1, "Молочные продукты"),
            (2, "Мясо и рыба"),
            (3, "Бакалея")
        ]
        links_data = [
            (1, 1),
            (1, 3),
            (2, 1),
            (2, 2)
        ]

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        expected_result = [
            ("Сыр", "Молочные продукты"),
            ("Сыр", "Бакалея"),
            ("Рыба", "Молочные продукты"),
            ("Рыба", "Мясо и рыба")
        ]

        result_df = get_product_category_pairs(products_df, categories_df, links_df)
        self.assert_dataframes_equal(result_df, expected_result)

    def test_categories_without_products(self):
        """Тест 4: Есть категории без продуктов"""
        products_data = [
            (1, "Яблоки"),
            (2, "Молоко")
        ]
        categories_data = [
            (1, "Фрукты"),
            (2, "Молочные продукты"),
            (3, "Мясо и рыба")
        ]
        links_data = [
            (1, 1),
            (2, 2)
        ]

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        expected_result = [
            ("Яблоки", "Фрукты"),
            ("Молоко", "Молочные продукты")
        ]

        result_df = get_product_category_pairs(products_df, categories_df, links_df)
        self.assert_dataframes_equal(result_df, expected_result)

    def test_empty_datasets(self):
        """Тест 5: Пустые датасеты"""
        products_data = []
        categories_data = []
        links_data = []

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        expected_result = []

        result_df = get_product_category_pairs(products_df, categories_df, links_df)
        self.assert_dataframes_equal(result_df, expected_result)

    def test_mixed_scenario(self):
        """Тест 6: Смешанный сценарий"""
        products_data = [
            (1, "Яблоки"),
            (2, "Молоко"),
            (3, "Хлеб"),
            (4, "Сыр"),
            (5, "Рыба")
        ]
        categories_data = [
            (1, "Фрукты"),
            (2, "Молочные продукты"),
            (3, "Мясо и рыба")
        ]
        links_data = [
            (1, 1),
            (2, 2),
            (4, 2),
            (4, 3),
            (5, 3)
        ]

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        expected_result = [
            ("Яблоки", "Фрукты"),
            ("Молоко", "Молочные продукты"),
            ("Хлеб", None),
            ("Сыр", "Молочные продукты"),
            ("Сыр", "Мясо и рыба"),
            ("Рыба", "Мясо и рыба")
        ]

        result_df = get_product_category_pairs(products_df, categories_df, links_df)
        self.assert_dataframes_equal(result_df, expected_result)


if __name__ == "__main__":
    unittest.main(verbosity=2)
