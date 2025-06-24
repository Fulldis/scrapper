import requests
import yake
import psycopg2
import json
from typing import List, Dict, Any
from flask import Flask, jsonify, request
import os

app = Flask(__name__)

# Configuration
GRAPHQL_URL = "http://scrapping:9003/api/v1/scrapping/graph/query"
DB_CONFIG = {
    'host': 'pgdb',
    'port': 5432,
    'database': 'scrapping',
    'user': 'articles',
    'password': 'articles'
}

GRAPHQL_QUERY = """
query GetArticles($page: Int!, $pageSize: Int!) {
  articles(page: $page, pageSize: $pageSize) {
    items {
      id
      name
      text
      complexity
      readingTime
      tags
      likes
      likedByUser
    }
    pageInfo {
      page
      pageSize
      hasNextPage
      hasPreviousPage
    }
  }
}
"""

class ArticleTagService:
    def __init__(self):
        self.keyword_extractor = yake.KeywordExtractor(
            lan="ru",
            n=2,
            dedupLim=0.5,
            windowsSize=1,
            top=10,
        )
        self.init_database()
    
    def init_database(self):
        """Initialize database and create tags table if it doesn't exist"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scrapping.article_tags (
                    id SERIAL PRIMARY KEY,
                    article_id INTEGER NOT NULL,
                    tags JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(article_id)
                );
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            print("Database initialized successfully")
            
        except Exception as e:
            print(f"Database initialization error: {str(e)}")
    
    def extract_keywords(self, text: str) -> List[str]:
        """Extract keywords from article text using YAKE"""
        if not isinstance(text, str) or len(text.strip()) == 0:
            return []
        try:
            keywords = self.keyword_extractor.extract_keywords(text)
            return [kw[0] for kw in keywords]
        except Exception as e:
            print(f"Error extracting keywords: {str(e)}")
            return []
    
    def save_tags_to_db(self, article_id: int, tags: List[str]) -> bool:
        """Save extracted tags to database"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            
            # Convert tags list to JSON
            tags_json = json.dumps(tags, ensure_ascii=False)
            
            # Insert or update tags for the article
            cursor.execute("""
                INSERT INTO scrapping.article_tags (article_id, tags)
                VALUES (%s, %s)
                ON CONFLICT (article_id) 
                DO UPDATE SET tags = EXCLUDED.tags, created_at = CURRENT_TIMESTAMP
            """, (article_id, tags_json))
            
            conn.commit()
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            print(f"Error saving tags to database: {str(e)}")
            return False
    
    def get_articles_from_graphql(self, page: int = 1, page_size: int = 10) -> Dict[str, Any]:
        """Fetch articles from GraphQL API"""
        variables = {"page": page, "pageSize": page_size}
        
        try:
            response = requests.post(
                GRAPHQL_URL,
                json={"query": GRAPHQL_QUERY, "variables": variables},
                headers={"Content-Type": "application/json"},
                timeout=15
            )
            
            if response.status_code != 200:
                return {"error": f"GraphQL request failed: {response.status_code}"}
                
            return response.json()
            
        except requests.exceptions.RequestException as e:
            return {"error": f"Network error: {str(e)}"}
        except Exception as e:
            return {"error": f"Unexpected error: {str(e)}"}
    
    def process_all_articles(self) -> Dict[str, Any]:
        """Process all articles and extract tags"""
        page = 1
        page_size = 20
        processed_count = 0
        error_count = 0
        results = []
        
        while True:
            data = self.get_articles_from_graphql(page, page_size)
            
            if "errors" in data:
                return {"error": data["errors"]}
            
            articles = data["data"]["articles"]["items"]
            page_info = data["data"]["articles"]["pageInfo"]
            
            # Process each article
            for article in articles:
                try:
                    keywords = self.extract_keywords(article["text"])
                    
                    if self.save_tags_to_db(article["id"], keywords):
                        processed_count += 1
                        results.append({
                            "article_id": article["id"],
                            "article_name": article["name"],
                            "tags": keywords
                        })
                    else:
                        error_count += 1
                        
                except Exception as e:
                    error_count += 1
                    print(f"Error processing article {article['id']}: {str(e)}")
            
            if not page_info["hasNextPage"]:
                break
                
            page += 1
        
        return {
            "processed_count": processed_count,
            "error_count": error_count,
        }

# Initialize the service
tag_service = ArticleTagService()

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "article_tag_service"})

@app.route('/extract-tags', methods=['POST'])
def extract_tags():
    """Extract tags for all articles"""
    try:
        result = tag_service.process_all_articles()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False) 