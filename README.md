# product_category_pairs

Минимальный пример на PySpark для вычисления пар "продукт-категория" с набором unit-тестов.

## Предварительные требования (Windows)
- Рекомендуется Python 3.10/3.11/3.12
- Java JDK 17 (Temurin) для Spark

Установка JDK 17 (с помощью winget):
```powershell
winget install EclipseAdoptium.Temurin.17.JDK
```
Настройка переменных окружения для текущей сессии (при необходимости):
```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17\"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```

## Установка и настройка
```powershell
cd C:\Users\Locadm\PycharmProjects\product_category_pairs
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Запуск приложения
```powershell
python main.py
```

## Запуск тестов
```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

## Структура проекта
```
product_category_pairs/
  main.py                # основной код приложения и пример использования
  tests/
    test_main.py         # unit-тесты для функции get_product_category_pairs
  requirements.txt       # зависимости проекта
  .gitignore             # файлы для игнорирования в Git
  README.md              # документация проекта
```
## Структура проекта
Приложение демонстрирует работу с PySpark для решения задачи:

Получение всех пар "Имя продукта - Имя категории"

Отображение продуктов, у которых нет категорий

Обработка различных сценариев: продукты без категорий, несколько категорий у продукта и т.д.

## Тестирование

Проект включает 6 unit-тестов, проверяющих различные сценарии:

Все продукты имеют категории

Есть продукты без категорий

Продукты с несколькими категориями

Есть категории без продуктов

Пустые наборы данных

Смешанный сценарий