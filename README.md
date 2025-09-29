# product_category_pairs

A minimal PySpark example that computes product-to-category pairs and includes a small unittest suite.

## Prerequisites (Windows)
- Python 3.10/3.11/3.12 recommended
- Java JDK 17 (Temurin) for Spark

Install JDK 17 (using winget):
```powershell
winget install EclipseAdoptium.Temurin.17.JDK
```
Set environment for current shell (if needed):
```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17\"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```

## Setup
```powershell
cd C:\Users\Locadm\PycharmProjects\product_category_pairs
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Run example
```powershell
python main.py
```

## Run tests
```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

## Project structure
```
product_category_pairs/
  main.py                # app code and example runner
  tests/
    test_main.py         # unittests for get_product_category_pairs
  requirements.txt
  .gitignore
  README.md
```

