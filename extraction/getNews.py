import os
from dotenv import load_dotenv
import logging
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

load_dotenv()

class GetNews:
    def __init__(self):
        self.api_key = os.getenv('NEWS_API_KEY')
        if not self.api_key:
            logging.error("API key not found. Check your .env file.")
            raise ValueError("Missing API Key.")

    def fetch_news(self, api_news_endpoint, keywords, language, from_date, category, to_date=None):
        try:
            url = api_news_endpoint

            # Define request parameters
            params = {
                'api-key': self.api_key,
                'text': ' OR '.join(keywords),  # Combine keywords into a single string
                'language': language,
                'from-date': from_date,
                'to-date': to_date,
                'category': ','.join(category)  # Convert list to comma-separated values
            }

            logging.info(f"Requesting news with parameters: {params}")

            # Make the API request
            response = requests.get(url, params=params)
            response.raise_for_status()

            # Debugging: Inspect response
            logging.info(f"Response URL: {response.url}")
            logging.info(f"Response Status Code: {response.status_code}")

            # Parse headlines from the response
            data = response.json()
            logging.info(f"Response Data: {data}")  # Log full response for debugging

            articles = data.get('response', {}).get('results', [])
            if not articles:
                logging.info("No articles found for the given criteria.")
                return []

            # Extract headlines
            headlines = [article.get('webTitle', 'N/A') for article in articles]
            for headline in headlines:
                print(f"Headline: {headline}")

            return headlines

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch news: {e}")
            return []

# Example usage
api_news_endpoint = "https://api.worldnewsapi.com/search-news"
keywords = ['نفت']
language = "fa"
from_date = "2022-01-01"
to_date = "2025-01-01"
category = ['Politics']

news = GetNews()
headlines = news.fetch_news(api_news_endpoint, keywords, language, from_date, to_date, category)
print(f"Total Headlines Found: {len(headlines)}")
