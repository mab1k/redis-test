import hashlib
import json
from flask import Flask, jsonify
from unittest.mock import patch
import redis

def client():
    app = Flask(__name__)
    from interface.docPage import api_docPage
    app.register_blueprint(api_docPage)
    return app.test_client()

def conn_redis():
    r = redis.Redis(host='localhost', port=6379, db=0)
    return r

def get_main_doc_info_success(client, conn_redis):
    with patch('authservice.authservice.checkHeaderToken') as mock_auth:
        mock_auth.return_value = (True, 'test_user')

        # ПРАВИЛЬНЫЙ МОК — мокаем метод у ГЛОБАЛЬНОГО ОБЪЕКТА
        with patch('interface.docPage.db_adapter.getDocInfo') as mock_get_doc_info:
            mock_get_doc_info.return_value = (
                {
                    'id': 1,
                    'author': 'Test Author',
                    'title': 'Test Title',
                    'source': 'Test Source',
                    'type_doc_id': 1,
                    'doc_type_title': 'Article',
                    'theme_id': 1,
                    'theme_title': 'Test Theme'
                },
                [{'id': 1, 'title': 'Test Discipline'}]
            )

            response = client.get(
                '/doc_info?doc_id=1',
                headers={'Authorization': 'Bearer valid_token'}
            )


            print("Response status code:", response.status_code)

            assert response.status_code == 200
            data = response.get_json()
            assert data['doc']['title'] == 'Test Title'

            key = '/doc_info?doc_4лщ4дкдпрдеid=1'
            hashed_key = hashlib.sha256(key.encode('utf-8')).hexdigest()

            if conn_redis.exists(hashed_key):
                print("Взяли данные из Redis /doc_info?doc_id=1")
                return conn_redis.get(hashed_key).decode('utf-8')
            else:
                print("Положили данные в Redis /doc_info?doc_id=1")
                conn_redis.set(hashed_key, json.dumps(data))
                return data


def get_doc_content_success(client, conn_redis):
    with patch('authservice.authservice.checkHeaderToken') as mock_auth:
        mock_auth.return_value = (True, 'test_user')

        # Мокаем db_adapter.getDocContent
        with patch('interface.docPage.db_adapter.getDocContent') as mock_get_content:
            mock_get_content.return_value = [
                {"section": "Введение", "text": "Текст введения..."},
                {"section": "Глава 1", "text": "Основной текст..."}
            ]

            response = client.get(
                '/doc_content?doc_id=1',
                headers={'Authorization': 'Bearer valid_token'}
            )

            print("Response status code:", response.status_code)
            assert response.status_code == 200
            data = response.get_json()
            assert len(data['contents']) > 0

            key = '/doc_content?doc_id=1'
            hashed_key = hashlib.sha256(key.encode('utf-8')).hexdigest()

            if conn_redis.exists(hashed_key):
                print("Взяли данные из Redis /doc_content?doc_id=1")
                return json.loads(conn_redis.get(hashed_key).decode('utf-8'))
            else:
                print("Положили данные в Redis /doc_content?doc_id=1")
                conn_redis.set(hashed_key, json.dumps(data))
                return data

if __name__ == "__main__":
    print(get_doc_content_success(client(), conn_redis()))
    print(get_main_doc_info_success(client(), conn_redis()))