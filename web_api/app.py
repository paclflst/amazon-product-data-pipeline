from flask import Flask, jsonify, request
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import datetime
from utils.logging import get_logger

app = Flask(__name__)
logger = get_logger('web-api')

def get_db_connection():
    conn = psycopg2.connect(host=os.environ.get('PG_HOST', 'postgres'),
                            database=os.environ.get('PG_DATABASE', 'test'),
                            user=os.environ.get('PG_USERNAME', 'test'),
                            password=os.environ.get('PG_PASSWORD', 'postgres'))
    return conn


def get_top_records(conn, month: str, items_per_list: int, ASC=False):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT month, title, avg_rating, brand, price, item as "ASIN"
            FROM dm_ratings_by_month 
            WHERE month = '{month}'
            ORDER BY avg_rating {'ASC' if ASC else 'DESC'}
            LIMIT {items_per_list};
        """)
        records = cur.fetchall()
    return records


def get_most_improved_records(conn, month: str, prev: str, items_per_list: int):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT c.month, c.title, c.avg_rating, c.brand, c.price, c.item as "ASIN"
                , p.avg_rating avg_rating_prev_month 
            FROM dm_ratings_by_month c
                LEFT JOIN dm_ratings_by_month p ON p.item = c.item
            WHERE c.month = '{month}' AND p.month = '{prev}'
            ORDER BY c.avg_rating - p.avg_rating DESC
            LIMIT {items_per_list};
        """)
        records = cur.fetchall()
    return records


def resolve_params(args: {}) -> (str, str, int):
    month = request.args.get('month', default='2014-04', type=str)
    items_per_list = request.args.get('items_per_list', default=5, type=int)

    if items_per_list < 1 or items_per_list > 20:
        raise ValueError('Parameter items_per_list must be between 1 and 20')

    try:
        dt = datetime.datetime.strptime(month, '%Y-%m')
        prev_dt = dt - datetime.timedelta(days=1)
        prev = dt.strftime('%Y-%m')
    except ValueError:
        raise ValueError('Parameter month must be in format YYYY-MM')

    return month, prev, items_per_list


@app.route('/')
def index():
    try:
        logger.debug(request)
        month, prev, items_per_list = resolve_params(request.args)
        with get_db_connection() as conn:
            top_items = get_top_records(conn, month, items_per_list, ASC=False)
            bottom_items = get_top_records(
                conn, month, items_per_list, ASC=True)
            improved_items = get_most_improved_records(
                conn, month, prev, items_per_list)
        return jsonify(
            top_items=top_items, 
            bottom_items=bottom_items, 
            most_improved_items=improved_items, 
            _params = {
                'month': month, 
                'items_per_list': items_per_list
            }
        )
    except ValueError as e:
        logger.error(f'Monthly web api value error:\n{e}')
        return jsonify(result='Bad Request', error_message=str(e)), 400
    except Exception as e:
        logger.error(f'Monthly web api error:\n{e}')
        return jsonify(result='Internal Server Error', error_message=str(e)), 500


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
