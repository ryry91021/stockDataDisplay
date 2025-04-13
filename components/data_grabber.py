import yfinance as yf
from fredapi import Fred
import pandas as pd
from forex_python.converter import CurrencyRates
import requests
from config import FRED_API_KEY

def get_sp500_price():
    """
    Retrieves the current S&P 500 index price.

    Returns:
        float: The latest closing price of the S&P 500 index.
               Returns None if the price cannot be retrieved.
    """
    try:
        # Yahoo Finance ticker for S&P 500 is "^GSPC"
        sp500 = yf.Ticker("^GSPC")
        
        # Get historical data for the most recent day
        hist = sp500.history(period="1d")
        
        if hist.empty:
            raise ValueError("No data fetched for S&P 500 index.")
        
        # Use the last closing price from the historical data
        price = hist['Close'].iloc[-1]
        return price
    except Exception as e:
        print("Error retrieving S&P 500 index price:", e)
        return None

def get_sofr_rate(api_key):
    """
    Retrieves the latest SOFR (Secured Overnight Financing Rate) using the FRED API.

    Args:
        api_key (str): Your FRED API key.

    Returns:
        float: The latest available SOFR rate, or None if not retrieved.
    """
    try:
        # Initialize the Fred client with your API key
        fred = Fred(api_key=api_key)
        
        # Fetch the SOFR time series data from FRED.
        # The FRED series identifier for the SOFR rate is "SOFR".
        data = fred.get_series('SOFR')
        
        # Check if any data was returned
        if data.empty:
            raise ValueError("No data fetched for SOFR rate.")
        
        # Extract the most recent rate from the series
        sofr_rate = data.iloc[-1]
        return sofr_rate
    except Exception as e:
        print("Error retrieving SOFR rate:", e)
        return None

def get_treasury_yield_curve(api_key):
    """
    Retrieves the latest U.S. Treasury yields for various maturities using the FRED API and
    organizes them in a two-column pandas DataFrame.

    Args:
        api_key (str): Your FRED API key.

    Returns:
        pandas.DataFrame: A DataFrame with columns "Term" and "Rate" containing the latest
                          rates for each maturity.
                          Returns None if there was an error retrieving the data.
    """
    try:
        fred = Fred(api_key=api_key)

        # Mapping of term labels to FRED series IDs
        rates_mapping = {
            "3M": "DGS3MO",  # 3-Month Treasury
            "6M": "DGS6MO",  # 6-Month Treasury
            "1Y": "DGS1",    # 1-Year Treasury
            "2Y": "DGS2",    # 2-Year Treasury
            "3Y": "DGS3",    # 3-Year Treasury
            "5Y": "DGS5",    # 5-Year Treasury
            "7Y": "DGS7",    # 7-Year Treasury (if available)
            "10Y": "DGS10",  # 10-Year Treasury
            "20Y": "DGS20",  # 20-Year Treasury (if available)
            "30Y": "DGS30"   # 30-Year Treasury
        }

        results = []
        for term, series_id in rates_mapping.items():
            try:
                # Retrieve the time series for the treasury rate
                series = fred.get_series(series_id)
                if series.empty:
                    raise ValueError("No data returned for series ID: " + series_id)
                
                # Get the most recent non-NaN value from the series
                latest_value = series.dropna().iloc[-1]
                results.append((term, latest_value))
            except Exception as e:
                print(f"Error retrieving data for {term} ({series_id}):", e)
                # Optionally, you can decide whether to continue or return None.
                # For now, we simply skip this term.

        # Convert the list of tuples into a DataFrame with columns "Term" and "Rate"
        df = pd.DataFrame(results, columns=["Term", "Rate"])
        return df

    except Exception as e:
        print("Error retrieving treasury yield curve data:", e)
        return None

import pandas as pd
from forex_python.converter import CurrencyRates
# Assume get_sp500_price, get_sofr_rate, and get_treasury_yield_curve are defined above


def get_forex_matrix(currencies=None, reference_base="USD"):
    """
    Retrieves forex rates based on a reference base (default 'USD') from a free API
    and computes a conversion matrix for the given list of currencies.
    
    Parameters:
        currencies (list of str): A list of currency codes (e.g., ['USD', 'EUR', 'GBP']).
                                  If None, the function will return a matrix for all currencies
                                  available in the API response.
        reference_base (str): The base currency used to fetch exchange rates (default 'USD').
        
    Returns:
        pandas.DataFrame: A DataFrame where each cell [i, j] represents the conversion rate from
                          currency i to currency j.
                          
    Note: This function uses the free API provided by exchangerate-api.com.
    """
    # URL to get the latest forex data relative to the reference base currency.
    url = f"https://api.exchangerate-api.com/v4/latest/{reference_base}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # raise an exception for HTTP errors
        data = response.json()
    except requests.RequestException as e:
        print("Error fetching forex data:", e)
        return None
    
    rates = data.get("rates", {})
    if not rates:
        print("No rates data returned from the API.")
        return None

    # If no specific currencies are provided, use all available
    if currencies is None:
        currencies = list(rates.keys())
    else:
        # Only keep the currencies that are available in the rates response
        currencies = [cur for cur in currencies if cur in rates]
        if not currencies:
            print("None of the specified currencies were found in the API data.")
            return None

    # Build the conversion matrix:
    # The conversion from currency i to currency j is calculated as:
    #     rate(j)/rate(i)
    matrix = {}
    for i in currencies:
        row = {}
        rate_i = rates[i]
        for j in currencies:
            rate_j = rates[j]
            row[j] = rate_j / rate_i
        matrix[i] = row

    # Create and return a DataFrame for better display/manipulation.
    forex_matrix = pd.DataFrame(matrix).T
    return forex_matrix


# Example usage:
if __name__ == "__main__":
    # Specify the currencies you want in your forex matrix.
    currencies_list = ['USD', 'EUR', 'GBP', 'JPY']
    forex_df = get_forex_matrix(currencies=currencies_list)

    if forex_df is not None:
        print("Forex Conversion Matrix:")
        print(forex_df)


# Example usage:
if __name__ == "__main__":
    # Assuming these functions are defined elsewhere in your code:
    # get_sp500_price(), get_sofr_rate(api_key), get_treasury_yield_curve(api_key)
    
    # Retrieve and print S&P 500 price
    price = get_sp500_price()
    if price is not None:
        print(f"The current S&P 500 index price is: {price}")
    else:
        print("Could not retrieve the S&P 500 index price.")
        
    # Replace with your actual FRED API key.
    
    # Retrieve and print SOFR rate
    rate = get_sofr_rate(FRED_API_KEY)
    if rate is not None:
        print(f"The latest SOFR rate is: {rate}")
    else:
        print("Could not retrieve the SOFR rate.")
    
    # Retrieve and print the Treasury yield curve
    yield_df = get_treasury_yield_curve(FRED_API_KEY)
    if yield_df is not None:
        print("Latest Treasury Yields:")
        print(yield_df)
    else:
        print("Could not retrieve the treasury yield curve data.")
    
    currencies_list = ['USD', 'EUR', 'GBP', 'JPY']
    forex_df = get_forex_matrix(currencies=currencies_list)

    if forex_df is not None:
        print("Forex Conversion Matrix:")
        print(forex_df)
