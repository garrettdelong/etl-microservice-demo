import requests

from services.snowflake_loader import get_snowflake_connection, load_posts_to_snowflake
from config import JSONPLACEHOLDER_POSTS_URL

def main():
    response = requests.get(JSONPLACEHOLDER_POSTS_URL, timeout=30)
    response.raise_for_status()

    data = response.json()
    load_result = load_posts_to_snowflake(data)

    print("load_result:", load_result)
"""
    conn = get_snowflake_connection()
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        print(cur.fetchone())
    finally:
        cur.close()
        conn.close()
"""


if __name__ == "__main__":
    main()
