import requests as re
import pandas as pd
import itertools
from typing import List
from time import sleep

APPLICATION_KEY = 'YOUR_APPLICATION_KEY'
CLIENT_ID = 'YOUR_CLIENT_ID'
CLIENT_SECRET = 'YOUR_CLIENT_SECRET'
ARIBA_NETWORK_ID = 'YOUR_ARIBA_NETWORK_ID'

TOKEN_URL = (f'https://api.ariba.com/v2/oauth/token?'
             f'grant_type=client_credentials'
             f'&client_id={CLIENT_ID}'
             f'&client_secret={CLIENT_SECRET}')

PO_HEADER_URL = 'https://openapi.ariba.com/api/purchase-orders-supplier/v1/prod/orders?'
PO_ITEMS_URL = 'https://openapi.ariba.com/api/purchase-orders-supplier/v1/prod/items?$top=100&'

PO_NUMBER_INPUT_STR = 'Megrendelések: '
PO_STATUS_INPUT_STR = 'Státusz (New, Changed, Confirmed): '

FILE_NAME = 'megrendelesek.xlsx'


def request_access_token() -> str:
    return re.post(TOKEN_URL).json()['access_token']


def build_po_items_url(po_number: int, po_status: str, skipped_items: int) -> str:
    url = PO_ITEMS_URL

    url += f'$filter=documentNumber eq \'{po_number}\'' \
        if po_number is not None else None

    url += f'and orderStatus eq \'{po_status}\'' \
        if po_number is not None and po_status is not None \
        else f'$filter=orderStatus eq \'{po_status}\''

    url += f'&$skip={skipped_items}' \
        if (skipped_items is not None) else None

    return url


def organize_data(po_items: pd.DataFrame) -> pd.DataFrame:
    po_items['megnevezes'] = po_items['description']
    po_items['1'] = None
    po_items['2'] = None
    po_items['3'] = None
    po_items['4'] = None
    po_items['5'] = None
    po_items['po_pozicio'] = po_items['lineNumber']
    po_items['6'] = None
    po_items['mennyiseg'] = po_items['quantity']
    po_items['munkaszam'] = None
    po_items['7'] = None
    po_items['cikkszam'] = po_items['buyerPartId'].str.lstrip('0')
    po_items['megrendeles'] = po_items['documentNumber']
    po_items['szallitasi_datum'] = po_items['requestedDeliveryDate']
    po_items['megrendelo'] = po_items['itemShipToName']
    po_items['8'] = None
    po_items['egysegar'] = po_items['unitPrice.amount']

    return po_items


def send_request(po_number: int, po_status: str, headers: dict) -> list[dict]:
    skipped_items: int = 0
    is_last_page: bool = False

    while not is_last_page:
        url = build_po_items_url(po_number, po_status, skipped_items)
        response = re.get(url=url, headers=headers).json()
        is_last_page = response['lastPage']
        skipped_items += 100

        for po_item in response['content']:
            yield po_item


def request_po_items_data() -> pd.DataFrame:
    access_token: str = request_access_token()

    headers = {
        'X-ARIBA-NETWORK-ID': ARIBA_NETWORK_ID,
        'apiKey': APPLICATION_KEY,
        'Authorization': f'Bearer {access_token}'
    }

    po_numbers_from_input: List = [po_number.strip() for po_number in input(PO_NUMBER_INPUT_STR).split(',')]
    po_status_from_input: str = input(PO_STATUS_INPUT_STR).strip()

    merged_data = itertools.chain.from_iterable(
        send_request(po_number, po_status_from_input, headers)
        for po_number in po_numbers_from_input
    )

    return organize_data(pd.json_normalize(merged_data))


def write_po_items_to_excel():
    try:
        po_items: pd.DataFrame = request_po_items_data().sort_values(by=['documentNumber', 'lineNumber'])
    except Exception as e:
        print(e)
        sleep(10)
        exit()

    try:
        with pd.ExcelWriter(FILE_NAME, mode="w", engine="openpyxl") as writer:
            po_items.to_excel(writer, sheet_name="Sheet1", index=False, header=True)
    except FileNotFoundError:
        po_items.to_excel(FILE_NAME, index=False)


write_po_items_to_excel()
