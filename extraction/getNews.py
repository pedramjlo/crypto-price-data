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

            params = {
                'api-key': self.api_key,
                'text': ' OR '.join(keywords), 
                'language': language,
                'from-date': from_date,
                'to-date': to_date,
                'category': ','.join(category)  
            }

            logging.info(f"Requesting news with parameters: {params}")

            response = requests.get(url, params=params)
            response.raise_for_status()

            logging.info(f"Response URL: {response.url}")
            logging.info(f"Response Status Code: {response.status_code}")

            data = response.json()
            logging.info(f"Response Data: {data}") 

            articles = data.get('response', {}).get('results', [])
            if not articles:
                logging.info("No articles found for the given criteria.")
                return []


            headlines = [article.get('webTitle', 'N/A') for article in articles]
            for headline in headlines:
                logging.ingo(f"Headline: {headline}")
            return headlines

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch news: {e}")
            return []

    def save_to_file(self, filename, headlines):
        try:
            with open(filename, 'w', encoding='utf-8') as file:
                for headline in headlines:
                    file.write(headline + '\n')
            logging.info(f"Headlines successfully saved to {filename}")
        except Exception as e:
            logging.error(f"Failed to save headlines to file: {e}")

# Example usage
api_news_endpoint = "https://api.worldnewsapi.com/search-news"
keywords = ['bitcoin', 'price', 'surge', 'boom', 'increase']
language = "en"
from_date = "2022-01-01"
to_date = "2025-01-01"
category = ['Politics']

news = GetNews()
headlines = news.fetch_news(api_news_endpoint, keywords, language, from_date, to_date, category)

if headlines:
    news.save_to_file('headlines.txt', headlines)


