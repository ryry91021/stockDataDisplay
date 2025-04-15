import yfinance as yf
from fredapi import Fred
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2  # Ensure you have installed psycopg2-binary
import numpy as np
from config import FRED_API_KEY, DB_CONFIG

class FinancialDataFetcher:
    """
    A class to fetch various financial data points and push the data to a PostgreSQL
    database on Supabase. It also allows you to create the necessary database schema.
    
    Expected tables with example schema:
    
      -- sp500_prices:
         id SERIAL PRIMARY KEY,
         timestamp TIMESTAMP DEFAULT NOW(),
         price NUMERIC

      -- sofr_rates:
         id SERIAL PRIMARY KEY,
         timestamp TIMESTAMP DEFAULT NOW(),
         rate NUMERIC

      -- treasury_rates:
         id SERIAL PRIMARY KEY,
         timestamp TIMESTAMP DEFAULT NOW(),
         term VARCHAR,
         rate NUMERIC

      -- forex_rates:
         id SERIAL PRIMARY KEY,
         timestamp TIMESTAMP DEFAULT NOW(),
         base_currency VARCHAR,
         target_currency VARCHAR,
         conversion_rate NUMERIC
    """
    
    def __init__(self, fred_api_key, reference_base="USD", db_config=None):
        """
        Initializes the FinancialDataFetcher with a FRED API key, reference base for forex rates,
        and an optional database configuration for a persistent PostgreSQL connection.

        Args:
            fred_api_key (str): Your FRED API key.
            reference_base (str): Base currency for forex rates (default "USD").
            db_config (dict, optional): Database connection parameters in the format:
                                        { "dbname": "...", "user": "...", "password": "...", "host": "...", "port": ... }
        """
        self.fred_api_key = fred_api_key
        self.reference_base = reference_base
        self.db_config = db_config
        self.conn = None
        self.cursor = None

        # If a database configuration is provided, open a persistent connection.
        if self.db_config:
            self._open_connection()

    def _open_connection(self):
        """Opens a persistent connection to the PostgreSQL database on Supabase."""
        try:
            self.conn = psycopg2.connect(
                dbname=self.db_config["dbname"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                host=self.db_config["host"],
                port=self.db_config.get("port", 5432)
            )
            self.cursor = self.conn.cursor()
            print("Persistent connection to Supabase established.")
        except Exception as e:
            print("Error establishing connection to Supabase:", e)
            self.conn = None
            self.cursor = None

    def close_connection(self):
        """Closes the persistent database connection."""
        if self.conn:
            self.cursor.close()
            self.conn.close()
            self.conn = None
            self.cursor = None
            print("Persistent connection closed.")

    def create_schema(self):
        """
        Creates the necessary tables in the PostgreSQL database if they don't exist.
        
        Tables created:
          - sp500_prices
          - sofr_rates
          - treasury_rates
          - forex_rates
        """
        if not self.conn or not self.cursor:
            print("No persistent connection available. Please provide a valid db_config.")
            return

        try:
            # Create sp500_prices table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS sp500_prices (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    price NUMERIC
                );
            """)
            # Create sofr_rates table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS sofr_rates (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    rate NUMERIC
                );
            """)
            # Create treasury_rates table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS treasury_rates (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    term VARCHAR,
                    rate NUMERIC
                );
            """)
            # Create forex_rates table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS forex_rates (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    base_currency VARCHAR,
                    target_currency VARCHAR,
                    conversion_rate NUMERIC
                );
            """)
            self.conn.commit()
            print("Database schema created (or verified) successfully.")
        except Exception as e:
            print("Error creating schema:", e)
            self.conn.rollback()

    def get_sp500_price(self):
        """
        Retrieves the current S&P 500 index price.
        
        Returns:
            float: The latest closing price of the S&P 500 index, or None if unavailable.
        """
        try:
            sp500 = yf.Ticker("^GSPC")
            hist = sp500.history(period="1d")
            if hist.empty:
                raise ValueError("No data fetched for S&P 500 index.")
            price = hist['Close'].iloc[-1]
            return price
        except Exception as e:
            print("Error retrieving S&P 500 index price:", e)
            return None

    def get_sofr_rate(self):
        """
        Retrieves the latest SOFR rate using the FRED API.
        
        Returns:
            float: The latest available SOFR rate, or None if not retrieved.
        """
        try:
            fred = Fred(api_key=self.fred_api_key)
            data = fred.get_series('SOFR')
            if data.empty:
                raise ValueError("No data fetched for SOFR rate.")
            sofr_rate = data.iloc[-1]
            return sofr_rate
        except Exception as e:
            print("Error retrieving SOFR rate:", e)
            return None

    def get_treasury_rate(self, term):
        """
        Retrieves the latest U.S. Treasury yield rate for a specified term using the FRED API.
        
        Args:
            term (str): The treasury term (e.g., "3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "20Y", "30Y").
            
        Returns:
            float: The latest yield rate for the specified term, or None if an error occurs.
        """
        rates_mapping = {
            "3M": "DGS3MO",
            "6M": "DGS6MO",
            "1Y": "DGS1",
            "2Y": "DGS2",
            "3Y": "DGS3",
            "5Y": "DGS5",
            "7Y": "DGS7",
            "10Y": "DGS10",
            "20Y": "DGS20",
            "30Y": "DGS30"
        }
        if term not in rates_mapping:
            print(f"Error: Term '{term}' not recognized. Valid terms are: {list(rates_mapping.keys())}")
            return None

        series_id = rates_mapping[term]
        try:
            fred = Fred(api_key=self.fred_api_key)
            series = fred.get_series(series_id)
            if series.empty:
                raise ValueError("No data returned for series ID: " + series_id)
            latest_value = series.dropna().iloc[-1]
            return latest_value
        except Exception as e:
            print(f"Error retrieving data for {term} ({series_id}):", e)
            return None

    def get_all_treasury_rates(self):
        """
        Retrieves the latest U.S. Treasury yield rates for predefined terms.
        
        Returns:
            dict: A dictionary with terms as keys and their corresponding yield rates as values.
        """
        terms = ["3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "20Y", "30Y"]
        rates = {}

        with ThreadPoolExecutor(max_workers=len(terms)) as executor:
            future_to_term = {executor.submit(self.get_treasury_rate, term): term for term in terms}
            for future in as_completed(future_to_term):
                term = future_to_term[future]
                rate = future.result()
                rates[term] = rate
                if rate is not None:
                    print(f"{term}: {rate}")
                else:
                    print(f"{term}: No data available")
        return rates

    def get_forex_data(self, currencies=None):
        """
        Retrieves forex rates based on the reference base currency.
        
        Args:
            currencies (list of str, optional): List of currency codes. If None, data for all available currencies is fetched.
        
        Returns:
            dict: Dictionary where keys are tuples (base_currency, target_currency) and values are conversion rates.
        """
        url = f"https://api.exchangerate-api.com/v4/latest/{self.reference_base}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            print("Error fetching forex data:", e)
            return None

        rates = data.get("rates", {})
        if not rates:
            print("No forex rates returned from API.")
            return None

        if currencies is None:
            currencies = list(rates.keys())
        else:
            currencies = [cur for cur in currencies if cur in rates]
            if not currencies:
                print("None of the specified currencies were found in the API data.")
                return None

        forex_rates = {}
        for i in currencies:
            rate_i = rates[i]
            for j in currencies:
                rate_j = rates[j]
                conversion_rate = rate_j / rate_i
                forex_rates[(i, j)] = conversion_rate
        return forex_rates

    def print_forex_rates(self, currencies=None):
        """
        Retrieves forex rates and prints each conversion pair.
        
        Args:
            currencies (list of str, optional): List of currency codes to include.
        """
        forex_data = self.get_forex_data(currencies)
        if forex_data is None:
            return
        for (base, target), rate in forex_data.items():
            print(f"{base} to {target}: {rate:.4f}")

    def push_all_data_to_supabase(self, forex_currencies=None):
        """
        Pushes all collected data points to their respective Supabase tables using the persistent DB connection.
        
        Data pushed:
        - S&P 500 price (table: sp500_prices)
        - SOFR rate (table: sofr_rates)
        - Treasury rates (table: treasury_rates)
        - Forex conversion rates (table: forex_rates)
        
        Args:
            forex_currencies (list, optional): List of currency codes for forex data. If None, uses all available currencies.
        """
        # Ensure the connection is still open.
        if not self.conn or not self.cursor:
            if self.db_config:
                self._open_connection()
            else:
                print("No persistent database connection is available.")
                return

        try:
            # S&P 500 Price
            sp500_price = self.get_sp500_price()
            if sp500_price is not None:
                # Convert to native Python float
                sp500_price = float(sp500_price)
                self.cursor.execute(
                    "INSERT INTO sp500_prices (timestamp, price) VALUES (NOW(), %s);",
                    (sp500_price,)
                )
                print(f"Inserted S&P 500 price: {sp500_price}")
            else:
                print("Skipping S&P 500 price insert due to missing data.")

            # SOFR Rate
            sofr_rate = self.get_sofr_rate()
            if sofr_rate is not None:
                sofr_rate = float(sofr_rate)
                self.cursor.execute(
                    "INSERT INTO sofr_rates (timestamp, rate) VALUES (NOW(), %s);",
                    (sofr_rate,)
                )
                print(f"Inserted SOFR rate: {sofr_rate}")
            else:
                print("Skipping SOFR rate insert due to missing data.")

            # Treasury Rates
            treasury_rates = self.get_all_treasury_rates()
            if treasury_rates:
                for term, rate in treasury_rates.items():
                    if rate is not None:
                        # Convert each treasury rate to float
                        rate = float(rate)
                        self.cursor.execute(
                            "INSERT INTO treasury_rates (timestamp, term, rate) VALUES (NOW(), %s, %s);",
                            (term, rate)
                        )
                        print(f"Inserted Treasury rate for {term}: {rate}")
                    else:
                        print(f"Skipping Treasury rate insert for {term} due to missing data.")
            else:
                print("No treasury rate data to insert.")

            # Forex Rates
            forex_data = self.get_forex_data(forex_currencies)
            if forex_data:
                for (base, target), conversion_rate in forex_data.items():
                    # Convert forex conversion rate to float
                    conversion_rate = float(conversion_rate)
                    self.cursor.execute(
                        "INSERT INTO forex_rates (timestamp, base_currency, target_currency, conversion_rate) VALUES (NOW(), %s, %s, %s);",
                        (base, target, conversion_rate)
                    )
                print("Inserted forex rate data.")
            else:
                print("No forex rate data to insert.")

            # Commit all changes
            self.conn.commit()
            print("All data pushed successfully to Supabase.")

        except Exception as e:
            print("Error pushing data to Supabase:", e)
            self.conn.rollback()


# Example usage:
if __name__ == "__main__":
    # Replace with your actual FRED API key and Supabase DB config.

    
    # Create a FinancialDataFetcher instance with a persistent DB connection.
    fetcher = FinancialDataFetcher(fred_api_key=FRED_API_KEY, db_config=DB_CONFIG)
    
    # Create the required database schema (tables).
    fetcher.create_schema()
    
    # Every 5 minutes, you may push new data points to the database.
    fetcher.push_all_data_to_supabase(forex_currencies=['USD', 'EUR', 'GBP', 'JPY'])
    
    # When finished (or on program exit), you can close the persistent connection:
    # fetcher.close_connection()
