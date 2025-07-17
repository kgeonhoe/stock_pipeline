#!/usr/bin/env python3
"""
Validation script to check if the stock pipeline is properly configured
"""

import os
import sys
import importlib.util

def check_file_exists(file_path, description):
    """Check if a file exists"""
    if os.path.exists(file_path):
        print(f"✅ {description}: {file_path}")
        return True
    else:
        print(f"❌ {description}: {file_path} (NOT FOUND)")
        return False

def check_directory_exists(dir_path, description):
    """Check if a directory exists"""
    if os.path.exists(dir_path) and os.path.isdir(dir_path):
        print(f"✅ {description}: {dir_path}")
        return True
    else:
        print(f"❌ {description}: {dir_path} (NOT FOUND)")
        return False

def check_python_import(module_name, description):
    """Check if a Python module can be imported"""
    try:
        spec = importlib.util.find_spec(module_name)
        if spec is not None:
            print(f"✅ {description}: {module_name}")
            return True
        else:
            print(f"❌ {description}: {module_name} (NOT FOUND)")
            return False
    except Exception as e:
        print(f"❌ {description}: {module_name} (ERROR: {e})")
        return False

def check_configuration():
    """Check configuration file"""
    cf_path = "/home/runner/work/stock_pipeline/stock_pipeline/jobs/cf.py"
    if not check_file_exists(cf_path, "Configuration file"):
        return False
    
    try:
        sys.path.append("/home/runner/work/stock_pipeline/stock_pipeline/jobs")
        import cf
        
        # Check if credentials are configured
        if hasattr(cf, 'appkey') and cf.appkey != "YOUR_KIS_API_KEY":
            print("✅ KIS API credentials are configured")
            return True
        else:
            print("⚠️  KIS API credentials need to be configured in cf.py")
            return False
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False

def main():
    """Main validation function"""
    print("=" * 60)
    print("🔍 STOCK PIPELINE VALIDATION CHECK")
    print("=" * 60)
    
    base_path = "/home/runner/work/stock_pipeline/stock_pipeline"
    
    # Check core directories
    print("\n📁 Directory Structure:")
    directories = [
        (f"{base_path}/dags", "Airflow DAGs directory"),
        (f"{base_path}/jobs", "Core pipeline jobs directory"),
        (f"{base_path}/examples", "Examples directory"),
        (f"{base_path}/data", "Data directory"),
        (f"{base_path}/resources", "Resources directory"),
    ]
    
    dir_success = True
    for dir_path, description in directories:
        if not check_directory_exists(dir_path, description):
            dir_success = False
    
    # Check core files
    print("\n📄 Core Files:")
    files = [
        (f"{base_path}/dags/download-stock-data.py", "Main Airflow DAG"),
        (f"{base_path}/jobs/collector.py", "Stock data collector"),
        (f"{base_path}/jobs/stock_filter.py", "Stock data filter"),
        (f"{base_path}/jobs/stock_crawl_main.py", "Main pipeline script"),
        (f"{base_path}/jobs/cf.py", "Configuration file"),
        (f"{base_path}/docker-compose.yml", "Docker composition"),
        (f"{base_path}/requirements.txt", "Python dependencies"),
        (f"{base_path}/SETUP_GUIDE.md", "Setup guide"),
        (f"{base_path}/examples/stock_pipeline_demo.py", "Demo script"),
    ]
    
    file_success = True
    for file_path, description in files:
        if not check_file_exists(file_path, description):
            file_success = False
    
    # Check Python dependencies
    print("\n🐍 Python Dependencies:")
    dependencies = [
        ("pandas", "Data manipulation library"),
        ("pymysql", "MySQL connector"),
        ("pykis", "KIS API client (python-kis)"),
        ("pyspark", "Apache Spark for Python"),
    ]
    
    dep_success = True
    for module_name, description in dependencies:
        if not check_python_import(module_name, description):
            dep_success = False
    
    # Check configuration
    print("\n⚙️  Configuration:")
    config_success = check_configuration()
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 VALIDATION SUMMARY")
    print("=" * 60)
    
    all_checks = [
        (dir_success, "Directory structure"),
        (file_success, "Core files"),
        (dep_success, "Python dependencies"),
        (config_success, "Configuration"),
    ]
    
    overall_success = True
    for success, check_type in all_checks:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status}: {check_type}")
        if not success:
            overall_success = False
    
    print("\n" + "=" * 60)
    if overall_success:
        print("🎉 ALL CHECKS PASSED! The stock pipeline is ready to use.")
        print("🚀 To start the pipeline: docker-compose up -d")
        print("🌐 Access Airflow UI: http://localhost:8081")
    else:
        print("⚠️  Some checks failed. Please review the issues above.")
        print("📚 See SETUP_GUIDE.md for detailed instructions.")
    print("=" * 60)

if __name__ == "__main__":
    main()