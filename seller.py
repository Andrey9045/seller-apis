import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Получить список товаров магазина Озон.

    Функция отправляет запрос к API Озон для получения списка товаров 
    магазина с указанным идентификатором клиента и токеном продавца. 
    Возвращает результаты в виде списка.

    Args:
        last_id: Идентификатор последнего товара для пагинации.
        client_id: Идентификатор клиента для API.
        seller_token: Токен продавца для авторизации.

    Returns:
        list: Список товаров, полученный из API. Если товаров нет, возвращает пустой список.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Получить артикулы товаров.

    Функция отправляет запрос к API Озон для получения списка товаров.
    Собирает артикулы всех товаров и возвращает их в виде списка словарей.

    Args:
        client_id: Идентификатор клиента для API.
        seller_token: Токен продавца для авторизации.

    Returns:
        list: Список артикулов, полученный из API. Если списка нет, возвращает пустой список.
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """
    Отправляется запрос на обновление цен.
    Args:
        prices (list): Список словарей, каждый из которых содержит информацию о товаре и новой цене.
        client_id (str): Идентификатор клиента для API.
        seller_token (str): Токен продавца для авторизации.

    Returns:
        dict: Ответ API в формате JSON, содержащий информацию об обновлении цен. 
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Функция отправляет запрос на обновление остатков товаров в системе Ozon.
    Args:
        stocks (list): Список словарей, каждый из которых содержит информацию о товаре и остатках.
        client_id (str): Идентификатор клиента для API.
        seller_token (str): Токен продавца для авторизации.

    Returns:
        dict: Ответ API в формате JSON, содержащий информацию об обновлении остатков. 
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """
    Скачивает файл остатков с сайта Casio и возвращает список остатков часов.

    Функция загружает ZIP-файл с остатками часов, извлекает его, читает содержимое Excel-файла и 
    возвращает данные в виде списка словарей.

    Returns:
        list: Список остатков часов, где каждый элемент представляет собой словарь с информацией о часах.

    Raises:
        requests.HTTPError: Если запрос к сайту завершился неудачно.
        FileNotFoundError: Если файл 'ostatki.xls' не найден после извлечения.
        ValueError: Если файл 'ostatki.xls' не может быть прочитан как Excel.
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants



def create_stocks(watch_remnants, offer_ids):
    """
    Создает список остатков для предложений на основе данных о часах.

    Функция фильтрует остатки часов, загруженные из источника, и сопоставляет их с переданными идентификаторами предложений.
    Если количество часов превышает 10, устанавливается запас 100. Если количество равно 1, запас устанавливается в 0.
    Остальные часы добавляются с их фактическим количеством. Для предложений, не имеющих соответствующих остатков,
    устанавливается запас 0.

    Args:
        watch_remnants (list): Список словарей с остатками часов, где каждый словарь содержит информацию о коде и количестве.
        offer_ids (list): Список идентификаторов предложений, которые необходимо проверить на наличие остатков.

    Returns:
        list: Список словарей, содержащих идентификаторы предложений и соответствующие запасы.
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает список цен для предложений на основе данных о часах.

    Функция фильтрует часы по идентификаторам предложений и формирует список цен,
    включая информацию о валюте, старой цене и новой цене, преобразованной с помощью функции price_conversion.

    Args:
        watch_remnants (list): Список словарей с остатками часов, где каждый словарь содержит информацию о коде и цене.
        offer_ids (list): Список идентификаторов предложений, которые необходимо проверить на наличие соответствующих часов.
    Returns:
        list: Список словарей, содержащих информацию о ценах для предложений.
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену в числовой формат.

    Преобразует строку с ценой, удаляя все символы, кроме цифр, возвращает цену в виде строки без разделителей.

    price (str): Цена в строковом формате, например, "5'990.00 руб.".
    str: Цена в виде строки без разделителей. Например, "5990".
    Args:
        price (str): Цена в строковом формате, например, "5'990.00 руб.".

    Returns:
        str: Цена в виде строки без разделителей. Например, "5990".
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Разделить список lst на части по n элементов
    Args:
        lst (list): Исходный список, который необходимо разделить.
        n (int): Количество элементов в каждом подсписке.

    Yields:
        list: Подсписки исходного списка, содержащие до n элементов.
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
    Загружает цены на товары, разделяя их на партии для обновления.

    Функция получает идентификаторы предложений, создает цены на основе остатков
    и обновляет их в системе, отправляя данные партиями.

    Args:
        watch_remnants (list): Список остатков товаров для обновления цен.
        client_id (str): Идентификатор клиента, используемый для аутентификации.
        seller_token (str): Токен продавца для доступа к API.

    Returns:
        list: Список созданных цен для товаров.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Обновляет запасы товаров и возвращает список ненулевых запасов.

    Функция получает идентификаторы предложений, создает запасы на основе остатков
    и обновляет их в системе, возвращая только ненулевые запасы.

    Args:
        watch_remnants (list): Список остатков товаров для обновления запасов.
        client_id (str): Идентификатор клиента, используемый для аутентификации.
        seller_token (str): Токен продавца для доступа к API.

    Returns:
        tuple: Кортеж из двух элементов:
            - list: Список ненулевых запасов товаров.
            - list: Список всех созданных запасов.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """
    Основная функция для обновления остатков и цен товаров.
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
