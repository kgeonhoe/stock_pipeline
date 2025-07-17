#!/usr/bin/env python3
"""
Example script showing how to use the stock data pipeline
for collecting Korean stock market data.

This script demonstrates:
1. How to download all stock lists based on current date
2. How to collect daily stock data for those stocks
3. How to store the data in the database

Note: This requires valid KIS API credentials in cf.py
"""

import sys
import os
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta

# Add the jobs directory to the Python path
sys.path.append('/home/runner/work/stock_pipeline/stock_pipeline/jobs')

from collector import Collector
from stock_filter import StockFilter
import cf


def demonstrate_stock_list_download():
    """Demonstrate downloading all stock lists based on current date"""
    print("=" * 60)
    print("DEMONSTRATION: Stock List Download")
    print("=" * 60)
    
    try:
        # Initialize collector with KIS API credentials
        collector = Collector(
            appkey=cf.appkey,
            appsecret=cf.secretkey,
            virtual_accountYN=True,
            account_id=cf.account_id
        )
        
        print("✅ Collector initialized successfully")
        print(f"📅 Current date: {datetime.datetime.now().strftime('%Y-%m-%d')}")
        
        # Download all stock lists
        print("\n🔄 Downloading all stock lists from KIS API...")
        all_stocks = collector.kis_get_all_stock()
        
        print(f"📊 Total stocks found: {len(all_stocks)}")
        print(f"📈 KOSPI stocks: {len(all_stocks[all_stocks['stockCode'].str.startswith('0')])}")
        print(f"📈 KOSDAQ stocks: {len(all_stocks[all_stocks['stockCode'].str.startswith('1')])}")
        
        # Show sample data
        print("\n📋 Sample stock data:")
        print(all_stocks.head().to_string())
        
        # Add ETL metadata
        all_stocks['etldate'] = datetime.datetime.now().strftime('%Y%m%d')
        all_stocks['etlcheck'] = None
        
        print(f"\n✅ Stock list download completed successfully!")
        print(f"📅 ETL Date: {all_stocks['etldate'].iloc[0]}")
        
        return all_stocks
        
    except Exception as e:
        print(f"❌ Error during stock list download: {e}")
        print("\n💡 Tips:")
        print("- Ensure KIS API credentials are valid in cf.py")
        print("- Check network connectivity")
        print("- Verify API rate limits")
        return None


def demonstrate_daily_data_collection(sample_stock_code="005930"):
    """Demonstrate collecting daily stock data for a specific stock"""
    print("\n" + "=" * 60)
    print("DEMONSTRATION: Daily Stock Data Collection")
    print("=" * 60)
    
    try:
        # Initialize collector
        collector = Collector(
            appkey=cf.appkey,
            appsecret=cf.secretkey,
            virtual_accountYN=True,
            account_id=cf.account_id
        )
        
        print(f"📊 Collecting data for stock: {sample_stock_code} (Samsung Electronics)")
        
        # Define date range (last 30 days)
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=30)
        
        datefrom = start_date.strftime('%Y%m%d')
        dateto = end_date.strftime('%Y%m%d')
        
        print(f"📅 Date range: {datefrom} to {dateto}")
        
        # Collect stock data
        print("\n🔄 Collecting stock data...")
        stock_data = collector.kis_get_values(sample_stock_code, datefrom, dateto)
        
        if stock_data:
            stockdate, stockclose, stockopen, stockhigh, stocklow, stockvolume, stockpricevolume = stock_data
            
            # Create DataFrame for better visualization
            df = pd.DataFrame({
                'Date': stockdate,
                'Close': stockclose,
                'Open': stockopen,
                'High': stockhigh,
                'Low': stocklow,
                'Volume': stockvolume,
                'Trading_Value': stockpricevolume
            })
            
            print(f"📊 Data points collected: {len(df)}")
            print(f"💰 Latest close price: {stockclose[0]:,} KRW")
            print(f"📊 Average daily volume: {sum(stockvolume)/len(stockvolume):,.0f}")
            
            # Show sample data
            print("\n📋 Sample stock data (latest 5 days):")
            print(df.head().to_string())
            
            print(f"\n✅ Daily data collection completed successfully!")
            
            return df
            
        else:
            print("❌ No data returned from API")
            return None
            
    except Exception as e:
        print(f"❌ Error during daily data collection: {e}")
        return None


def demonstrate_automated_pipeline():
    """Demonstrate how the automated pipeline works"""
    print("\n" + "=" * 60)
    print("DEMONSTRATION: Automated Pipeline Process")
    print("=" * 60)
    
    print("🔄 This is how the daily automated pipeline works:")
    print("\n1. 📅 Daily Schedule: 15:31 KST (after market close)")
    print("2. 📊 Download all stock lists based on current date")
    print("3. 🔍 Identify stocks needing data collection:")
    print("   - New stocks since last run")
    print("   - Stocks with corporate actions (splits, etc.)")
    print("   - Stocks missing recent data")
    print("4. 📈 Collect historical data for identified stocks")
    print("5. 💾 Store data in MySQL database")
    print("6. 🧹 Clean up and maintain data quality")
    
    print("\n🏗️ Architecture Components:")
    print("- 🐳 Docker containers for all services")
    print("- 🌬️ Apache Airflow for orchestration")
    print("- ⚡ Apache Spark for data processing")
    print("- 🗄️ MySQL for data storage")
    print("- 📊 KIS API for stock data")
    
    print("\n📊 Data Management Features:")
    print("- ✅ Incremental loading (only new data)")
    print("- 🔄 Automatic corporate action handling")
    print("- 📅 Daily stock list updates")
    print("- 🛡️ Data integrity checks")
    print("- 📈 Historical data backfill")
    
    print("\n🎯 Use Cases:")
    print("- 📊 Daily stock market analysis")
    print("- 📈 Quantitative trading strategies")
    print("- 🔍 Market research and screening")
    print("- 📉 Risk management and monitoring")
    print("- 🤖 Machine learning model training")


def main():
    """Main function to run all demonstrations"""
    print("🚀 KOREAN STOCK DATA PIPELINE DEMONSTRATION")
    print("=" * 60)
    
    # Check if running with real credentials
    if cf.appkey == "YOUR_KIS_API_KEY" or len(cf.appkey) != 36:
        print("⚠️  DEMO MODE: Using placeholder credentials")
        print("💡 To run with real data, update cf.py with valid KIS API credentials")
        print("\n📖 This demonstration shows the pipeline structure and capabilities")
        
        # Show architecture and process
        demonstrate_automated_pipeline()
        
    else:
        print("🔐 Using configured KIS API credentials")
        
        # Run actual demonstrations
        stocks_df = demonstrate_stock_list_download()
        
        if stocks_df is not None:
            # Use first stock for data collection demo
            sample_stock = stocks_df.iloc[0]['stockCode']
            demonstrate_daily_data_collection(sample_stock)
        
        # Show pipeline overview
        demonstrate_automated_pipeline()
    
    print("\n" + "=" * 60)
    print("📚 For complete setup instructions, see SETUP_GUIDE.md")
    print("🐳 To start the pipeline: docker-compose up -d")
    print("🌐 Airflow UI: http://localhost:8081")
    print("=" * 60)


if __name__ == "__main__":
    main()