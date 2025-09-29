import unittest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from main import setup_spark_environment, create_spark_session, get_product_category_pairs


class TestProductCategoryPairs(unittest.TestCase):
    """Тесты для функции get_product_category_pairs"""

    @classmethod
    def setUpClass(cls):
        """Настройка Spark перед всеми тестами"""
        setup_spark_environment()
        cls.spark = create_spark_session()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        """Очистка после всех тестов"""
        cls.spark.stop()

    def create_test_dataframes(self, products_data, categories_data, links_data):
        """Создание тестовых датафреймов из данных"""

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
        """Проверка что датафрейм содержит ожидаемые данные"""
        result_data = sorted([(row['product_name'], row['category_name']) for row in result_df.collect()])
        expected_data_sorted = sorted(expected_data)

        self.assertEqual(result_data, expected_data_sorted)

    def test_all_products_have_categories(self):
        """Тест 1: Все продукты имеют категории"""
        products_data = [
            (1, "Laptop"),
            (2, "Mouse"),
            (3, "Keyboard")
        ]
        categories_data = [
            (1, "Electronics"),
            (2, "Accessories")
        ]
        links_data = [
            (1, 1),
            (2, 2),
            (3, 2)
        ]

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        expected_result = [
            ("Laptop", "Electronics"),
            ("Mouse", "Accessories"),
            ("Keyboard", "Accessories")
        ]

        result_df = get_product_category_pairs(products_df, categories_df, links_df)

        self.assert_dataframes_equal(result_df, expected_result)

    def test_products_without_categories(self):
        """Тест 2: Есть продукты без категорий"""
        products_data = [
            (1, "Laptop"),
            (2, "Mouse"),
            (3, "Keyboard"),
            (4, "Monitor")
        ]
        categories_data = [
            (1, "Electronics"),
            (2, "Accessories")
        ]
        links_data = [
            (1, 1),
            (2, 2)
        ]

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        expected_result = [
            ("Laptop", "Electronics"),
            ("Mouse", "Accessories"),
            ("Keyboard", None),
            ("Monitor", None)
        ]

        result_df = get_product_category_pairs(products_df, categories_df, links_df)

        self.assert_dataframes_equal(result_df, expected_result)

    def test_products_with_multiple_categories(self):
        """Тест 3: Продукты с несколькими категориями"""
        products_data = [
            (1, "Laptop"),
            (2, "Mouse")
        ]
        categories_data = [
            (1, "Electronics"),
            (2, "Accessories"),
            (3, "Office Equipment")
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
            ("Laptop", "Electronics"),
            ("Laptop", "Office Equipment"),
            ("Mouse", "Electronics"),
            ("Mouse", "Accessories")
        ]

        result_df = get_product_category_pairs(products_df, categories_df, links_df)

        self.assert_dataframes_equal(result_df, expected_result)

    def test_categories_without_products(self):
        """Тест 4: Есть категории без продуктов"""
        products_data = [
            (1, "Laptop"),
            (2, "Mouse")
        ]
        categories_data = [
            (1, "Electronics"),
            (2, "Accessories"),
            (3, "Office Equipment")
        ]
        links_data = [
            (1, 1),
            (2, 2)
        ]

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        expected_result = [
            ("Laptop", "Electronics"),
            ("Mouse", "Accessories")
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
            (1, "Laptop"),
            (2, "Mouse"),
            (3, "Keyboard"),
            (4, "Monitor"),
            (5, "Webcam")
        ]
        categories_data = [
            (1, "Electronics"),
            (2, "Accessories"),
            (3, "Office Equipment")
        ]
        links_data = [
            (1, 1),
            (2, 2),
            (4, 1),
            (4, 3)
        ]

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        expected_result = [
            ("Laptop", "Electronics"),
            ("Mouse", "Accessories"),
            ("Keyboard", None),
            ("Monitor", "Electronics"),
            ("Monitor", "Office Equipment"),
            ("Webcam", None)
        ]

        result_df = get_product_category_pairs(products_df, categories_df, links_df)

        self.assert_dataframes_equal(result_df, expected_result)


if __name__ == "__main__":
    unittest.main(verbosity=2)


