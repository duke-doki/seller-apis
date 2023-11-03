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
    """Получить список товаров магазина озон.

        Args:
            last_id (int): id последнего продукта
            client_id (str): id клиента
            seller_token (str): токен продавца

        Returns:
            dict: словарь с продуктами

        Raises:
            requests.exceptions.InvalidHeader: если неправильно указаны
                id или токен

        Examples:

            >>> print(get_product_list(last_id, client_id, seller_token))
                "items": [
                    {
                        "product_id": 223681945,
                        "offer_id": "136748"
                    }
                ],
                "total": 1,
                "last_id": "bnVсbA=="
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
    """Получить артикулы товаров магазина озон

        Args:
            client_id (int): id клиента
            seller_token (str): токен продавца

        Returns:
            list: список с продуктами

        Raises:
            requests.exceptions.InvalidHeader: если неправильно указаны
                id или токен

        Examples:
            >>> print(get_offer_ids(client_id, seller_token))
            [136748,]

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
    """Обновить цены товаров

        Args:
            prices (list): список цен
            client_id (str): id клиента
            seller_token (str): токен продавца

        Returns:
            dict: возвращает обновленный словарь из цен на озоне

        Raises:
            requests.exceptions.InvalidHeader: если неправильно указаны
                id или токен
            AttributeError: если аргумент prices не list

        Examples:
            >>> print(update_price(prices: list, client_id, seller_token))
                {
                    "result":

                        [

                            {
                                "product_id": 1386,
                                "offer_id": "PH8865",
                                "updated": true,
                                "errors": []
                            }
                        ]

                }
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
    """Обновить остатки

            Args:
                stocks (list): список остатков продуктов
                client_id (str): id клиента
                seller_token (str): токен продавца

            Returns:
                dict: возвращает обновленный словарь из остатков на озоне

            Raises:
                requests.exceptions.InvalidHeader: если неправильно указаны
                    id или токен
                AttributeError: если аргумент stocks не list

            Examples:
                >>> print(update_stocks(stocks: list, client_id, seller_token))
                    {
                        "result": [
                            {
                                "product_id": 55946,
                                "offer_id": "PG-2404С1",
                                "updated": true,
                                "errors": []
                            }
                        ]
                    }
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
    """Скачать файл ostatki с сайта casio

               Returns:
                   dict: возвращает словарь остатков на casio

       """

    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")

    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """создать остатки

            Args:
                watch_remnants (dict): словарь остатков на casio
                offer_ids (list): список с продуктами

            Returns:
                dict: возвращает обновленный список словарей с остатками

            Raises:
                requests.exceptions.InvalidHeader: если неправильно указаны
                    id или токен

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
    """Сформировать цены

             Args:
                 watch_remnants (dict): словарь остатков на casio
                 offer_ids (list): список с продуктами

             Returns:
                 dict: возвращает обновленный список словарей с
                    ценами на часы

             Raises:
                 requests.exceptions.InvalidHeader: если неправильно указаны
                     id или токен

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
    """Преобразовать цену. Пример: 5'990.00 руб. -> 5990.

        Args:
            price (str): цена в формате str

        Returns:
            str: пребразованная цена.

        Raises:
            AttributeError: если аргумент не str

        Examples:

            >>> import re
            >>> print(price_conversion("5'990.00 руб."))
            5990

        """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов

        Args:
            lst (str): список
            n (int): количество элементов

        Yields:
            str: списки с элементами

        Raises:
            AttributeError: если аргумент lst не list,
                n не int

        Examples:

            >>> res = divide(some_list, 2)
            >>> for s in res:
            ...     print(s)
                [1, 2]
                [3, 4]
                [5, 6]
                [7, 8]

        """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """загрузить цены

            Args:
                watch_remnants (dict): словарь остатков на casio
                client_id (str): id клиента
                seller_token (str): токен продавца

            Returns:
                dict: возвращает обновленный список словарей с ценами

            Raises:
                requests.exceptions.InvalidHeader: если неправильно указаны
                    id или токен

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """загрузить остатки

            Args:
                watch_remnants (dict): словарь остатков на casio
                client_id (str): id клиента
                seller_token (str): токен продавца

            Returns:
                dict: возвращает обновленный список словарей с остатками

            Raises:
                requests.exceptions.InvalidHeader: если неправильно указаны
                    id или токен

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
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
