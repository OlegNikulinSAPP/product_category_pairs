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