import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col
import unittest


def setup_spark_environment():
    """Настройка окружения Spark для Windows"""
    # Очистка переменных окружения
    for key in ['PYSPARK_PYTHON', 'PYSPARK_DRIVER_PYTHON', 'HADOOP_HOME']:
        if key in os.environ:
            del os.environ[key]

    # Используем системный Python
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def create_spark_session():
    """Создание SparkSession с правильными настройками"""
    return SparkSession.builder \
        .appName("ProductCategoryApp") \
        .master("local[1]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


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
        print("Тест 1: Все продукты имеют категории")

        # Данные
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
            (1, 1),  # Laptop -> Electronics
            (2, 2),  # Mouse -> Accessories
            (3, 2)  # Keyboard -> Accessories
        ]

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        # Ожидаемый результат
        expected_result = [
            ("Laptop", "Electronics"),
            ("Mouse", "Accessories"),
            ("Keyboard", "Accessories")
        ]

        # Вызов функции
        result_df = get_product_category_pairs(products_df, categories_df, links_df)

        # Проверка
        self.assert_dataframes_equal(result_df, expected_result)
        print("✓ Пройден")

    def test_products_without_categories(self):
        """Тест 2: Есть продукты без категорий"""
        print("Тест 2: Есть продукты без категорий")

        # Данные
        products_data = [
            (1, "Laptop"),
            (2, "Mouse"),
            (3, "Keyboard"),  # Без категории
            (4, "Monitor")  # Без категории
        ]
        categories_data = [
            (1, "Electronics"),
            (2, "Accessories")
        ]
        links_data = [
            (1, 1),  # Laptop -> Electronics
            (2, 2)  # Mouse -> Accessories
            # Keyboard и Monitor без категорий
        ]

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        # Ожидаемый результат
        expected_result = [
            ("Laptop", "Electronics"),
            ("Mouse", "Accessories"),
            ("Keyboard", None),  # Продукты без категорий
            ("Monitor", None)
        ]

        # Вызов функции
        result_df = get_product_category_pairs(products_df, categories_df, links_df)

        # Проверка
        self.assert_dataframes_equal(result_df, expected_result)
        print("✓ Пройден")

    def test_products_with_multiple_categories(self):
        """Тест 3: Продукты с несколькими категориями"""
        print("Тест 3: Продукты с несколькими категориями")

        # Данные
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
            (1, 1),  # Laptop -> Electronics
            (1, 3),  # Laptop -> Office Equipment
            (2, 1),  # Mouse -> Electronics
            (2, 2)  # Mouse -> Accessories
        ]

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        # Ожидаемый результат
        expected_result = [
            ("Laptop", "Electronics"),
            ("Laptop", "Office Equipment"),
            ("Mouse", "Electronics"),
            ("Mouse", "Accessories")
        ]

        # Вызов функции
        result_df = get_product_category_pairs(products_df, categories_df, links_df)

        # Проверка
        self.assert_dataframes_equal(result_df, expected_result)
        print("✓ Пройден")

    def test_categories_without_products(self):
        """Тест 4: Есть категории без продуктов"""
        print("Тест 4: Есть категории без продуктов")

        # Данные
        products_data = [
            (1, "Laptop"),
            (2, "Mouse")
        ]
        categories_data = [
            (1, "Electronics"),
            (2, "Accessories"),
            (3, "Office Equipment")  # Категория без продуктов
        ]
        links_data = [
            (1, 1),  # Laptop -> Electronics
            (2, 2)  # Mouse -> Accessories
        ]

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        # Ожидаемый результат
        expected_result = [
            ("Laptop", "Electronics"),
            ("Mouse", "Accessories")
        ]

        # Вызов функции
        result_df = get_product_category_pairs(products_df, categories_df, links_df)

        # Проверка
        self.assert_dataframes_equal(result_df, expected_result)
        print("✓ Пройден")

    def test_empty_datasets(self):
        """Тест 5: Пустые датасеты"""
        print("Тест 5: Пустые датасеты")

        # Пустые данные
        products_data = []
        categories_data = []
        links_data = []

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        # Ожидаемый результат
        expected_result = []

        # Вызов функции
        result_df = get_product_category_pairs(products_df, categories_df, links_df)

        # Проверка
        self.assert_dataframes_equal(result_df, expected_result)
        print("✓ Пройден")

    def test_mixed_scenario(self):
        """Тест 6: Смешанный сценарий"""
        print("Тест 6: Смешанный сценарий")

        # Данные
        products_data = [
            (1, "Laptop"),
            (2, "Mouse"),
            (3, "Keyboard"),  # Без категории
            (4, "Monitor"),  # Несколько категорий
            (5, "Webcam")  # Без категории
        ]
        categories_data = [
            (1, "Electronics"),
            (2, "Accessories"),
            (3, "Office Equipment")
        ]
        links_data = [
            (1, 1),  # Laptop -> Electronics
            (2, 2),  # Mouse -> Accessories
            (4, 1),  # Monitor -> Electronics
            (4, 3)  # Monitor -> Office Equipment
        ]

        products_df, categories_df, links_df = self.create_test_dataframes(
            products_data, categories_data, links_data
        )

        # Ожидаемый результат
        expected_result = [
            ("Laptop", "Electronics"),
            ("Mouse", "Accessories"),
            ("Keyboard", None),
            ("Monitor", "Electronics"),
            ("Monitor", "Office Equipment"),
            ("Webcam", None)
        ]

        # Вызов функции
        result_df = get_product_category_pairs(products_df, categories_df, links_df)

        # Проверка
        self.assert_dataframes_equal(result_df, expected_result)
        print("✓ Пройден")


def main():
    """Основная функция с примером использования"""
    setup_spark_environment()

    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("ERROR")

        print("🚀 ПРИМЕР ИСПОЛЬЗОВАНИЯ ФУНКЦИИ get_product_category_pairs")
        print("=" * 60)

        # Создаем тестовые данные
        products_data = [
            (1, "Laptop"),
            (2, "Mouse"),
            (3, "Keyboard"),  # Без категории
            (4, "Monitor"),  # Несколько категорий
            (5, "Webcam")  # Без категории
        ]

        categories_data = [
            (1, "Electronics"),
            (2, "Accessories"),
            (3, "Office Equipment")
        ]

        links_data = [
            (1, 1),  # Laptop -> Electronics
            (2, 2),  # Mouse -> Accessories
            (4, 1),  # Monitor -> Electronics
            (4, 3)  # Monitor -> Office Equipment
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
        print("\n📊 ИСХОДНЫЕ ДАННЫЕ:")
        print("\n--- ПРОДУКТЫ ---")
        products_df.show()

        print("\n--- КАТЕГОРИИ ---")
        categories_df.show()

        print("\n--- СВЯЗИ ---")
        links_df.show()

        # Вызываем нашу функцию
        print("\n🔧 ВЫЗОВ ФУНКЦИИ get_product_category_pairs...")
        result_df = get_product_category_pairs(products_df, categories_df, links_df)

        # Показываем результат
        print("\n🎯 РЕЗУЛЬТАТ:")
        result_df.show()

        # Анализ результатов
        results = result_df.collect()

        print("\n📈 АНАЛИЗ РЕЗУЛЬТАТОВ:")
        total_pairs = len(results)
        products_with_categories = len([r for r in results if r['category_name'] is not None])
        products_without_categories = len([r for r in results if r['category_name'] is None])

        print(f"• Всего пар: {total_pairs}")
        print(f"• Продуктов с категориями: {products_with_categories}")
        print(f"• Продуктов без категорий: {products_without_categories}")

        # Показываем продукты без категорий
        if products_without_categories > 0:
            print("\n⚠️  ПРОДУКТЫ БЕЗ КАТЕГОРИЙ:")
            for row in results:
                if row['category_name'] is None:
                    print(f"   - {row['product_name']}")

        print("\n" + "=" * 60)
        print("✅ ПРИМЕР ЗАВЕРШЕН УСПЕШНО!")

        spark.stop()

    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Запуск примера использования
    main()

    print("\n" + "=" * 60)
    print("🧪 ЗАПУСК ТЕСТОВ...")
    print("=" * 60)

    # Запуск тестов
    unittest.main(argv=[''], verbosity=2, exit=False)