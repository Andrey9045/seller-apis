import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получает список товаров из API Яндекс.Маркета.

    Args:
        page (str): Токен страницы для доступа к результатам.
        campaign_id (str): Идентификатор кампании, для которой запрашиваются товары.
        access_token (str): Токен доступа для аутентификации в API.

    Returns:
        list: Список товаров в формате JSON, полученный из API.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Обновляет запасы товаров API Яндекс.Маркета.

    Args:
        stocks (list): Список товаров с обновленными запасами.
        campaign_id (str): Идентификатор для которого обновляются запасы.
        access_token (str): Токен доступа для аутентификации в API.

    Returns:
        dict: Ответ от API в формате JSON, содержащий информацию об обновленных запасах.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Обновляет цены на товары для API Яндекс.Маркета.

    Args:
        prices (list): Список объектов с обновленными ценами на товары.
        campaign_id (str): Идентификатор кампании, для которой обновляются цены.
        access_token (str): Токен доступа для аутентификации в API.

    Returns:
        dict: Ответ от API в формате JSON, содержащий информацию об обновленных ценах.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Получить артикулы товаров.

    Функция отправляет запрос к API Озон для получения списка товаров.
    Собирает артикулы всех товаров и возвращает их в виде списка словарей.

    Args:
        campaign_id: Идентификатор клиента для API.
        market_token: Токен продавца для авторизации.

    Returns:
        list: Список артикулов, полученный из API. Если списка нет, возвращает пустой список.
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Создает список остатков товаров на складе на основе данных о остатках и идентификаторов предложений.

    Args:
        watch_remnants (list): Список словарей, содержащих информацию о остатках товаров (код и количество).
        offer_ids (list): Список идентификаторов предложений, которые необходимо проверить и обновить.
        warehouse_id (str): Идентификатор склада, к которому относятся остатки.

    Returns:
        list: Список словарей с информацией о товарах на складе, включая их количество и дату обновления.
    """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает список цен для предложений.

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
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Асинхронно загружает цены на товары в указанную кампанию на основе остатков и токена.

    Args:
        watch_remnants (list): Список словарей с информацией о остатках товаров (код и цена).
        campaign_id (str): Идентификатор кампании, в которую загружаются цены.
        market_token (str): Токен доступа к API маркетплейса для выполнения операций.

    Returns:
        list: Список словарей с обновленными ценами товаров.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Асинхронно загружает остатки товаров в указанную кампанию и склад.

    Args:
        watch_remnants (list): Список словарей с информацией о остатках товаров (код и количество).
        campaign_id (str): Идентификатор кампании, в которую загружаются остатки.
        market_token (str): Токен доступа к API маркетплейса для выполнения операций.
        warehouse_id (str): Идентификатор склада, на который загружаются остатки.

    Returns:
        tuple: Кортеж, содержащий два элемента:
            - list: Список словарей с остатками товаров, у которых количество больше 0.
            - list: Список всех загруженных остатков товаров.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
