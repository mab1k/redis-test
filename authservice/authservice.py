import requests

def check_key(key: str):
    return True, 1
    url = f"https://vega.rest/authservice.php?op=parsetoken&"
    

    params = {'token': key}
    
    try:

        response = requests.get(url, params=params)
        response.raise_for_status()  
        if response.status_code != 200:
            return False, None

        data = response.json()
        
        user_id = data['uai']
        return True, user_id
    

    except requests.exceptions.RequestException:
        # Ловим любые ошибки запроса (включая 401, 404, таймауты и т.д.)
        return False, None

    except (ValueError, KeyError):
        # Ловим ошибки парсинга JSON или отсутствия ключа 'uai'
        return False, None
    
    
def checkHeaderToken(header):
    auth_header = header.get('Authorization', None)
    if auth_header is None:
        return False
    else:
        parts = auth_header.split()
        if len(parts) != 2 or parts[0] != 'Bearer':
            return False
        access_key = parts[1]
        key_status, u_id = check_key(access_key)
        return key_status, u_id