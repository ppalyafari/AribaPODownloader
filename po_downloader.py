import requests as re
import pandas as pd
import itertools
from typing import List


class PODownloader:
    APPLICATION_KEY = 'YOUR_API_KEY'
    CLIENT_ID = 'YOUR_CLIENT_ID'
    CLIENT_SECRET = 'YOUR_CLIENT_SECRET'
    ARIBA_NETWORK_ID = 'YOUR_ANID'

    TOKEN_URL = (f'https://api.ariba.com/v2/oauth/token?'
                 f'grant_type=client_credentials'
                 f'&client_id={CLIENT_ID}'
                 f'&client_secret={CLIENT_SECRET}')

    ACCESS_TOKEN = None

    PO_HEADER_URL = 'https://openapi.ariba.com/api/purchase-orders-supplier/v1/prod/orders?'

    PO_ITEMS_URL = 'https://openapi.ariba.com/api/purchase-orders-supplier/v1/prod/items?$top=1000&'

    PO_NUMBER_INPUT_STR = 'Megrendelések: '

    PO_STATUS_INPUT_STR = 'Státusz (New, Changed, Confirmed): '

    def request_access_token(self) -> None:
        self.ACCESS_TOKEN = re.post(self.TOKEN_URL).json()['access_token']

    def build_po_items_url(self, po_number: int, po_status: str) -> str:
        url = self.PO_ITEMS_URL

        url += f'$filter=documentNumber eq \'{po_number}\'' \
                if po_number is not None else None

        url += f'and orderStatus eq \'{po_status}\'' \
            if po_number is not None and po_status is not None \
            else f'$filter=orderStatus eq \'{po_status}\''

        return url

    @staticmethod
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

    def request_po_items_data(self) -> pd.DataFrame:
        self.request_access_token()

        headers = {
            'X-ARIBA-NETWORK-ID': self.ARIBA_NETWORK_ID,
            'apiKey': self.APPLICATION_KEY,
            'Authorization': f'Bearer {self.ACCESS_TOKEN}'
        }

        po_numbers: List = [po_number.strip() for po_number in input(self.PO_NUMBER_INPUT_STR).split(',')]

        po_status: str = input(self.PO_STATUS_INPUT_STR).strip()

        po_items: List[List[dict]] = [re.get(url=self.build_po_items_url(po_number, po_status),
                                                   headers=headers).json()['content']
                                            for po_number in po_numbers]

        merged_data: List[dict] = itertools.chain.from_iterable(po_items)

        return PODownloader.organize_data(pd.json_normalize(merged_data))

    def write_po_items_to_excel(self):
        po_items: pd.DataFrame = self.request_po_items_data().sort_values(by=['documentNumber', 'lineNumber'])

        try:
            with pd.ExcelWriter('megrendelesek.xlsx', mode="w", engine="openpyxl") as writer:
                po_items.to_excel(writer, sheet_name="Sheet1", index=False, header=True)
        except FileNotFoundError:
            po_items.to_excel('megrendelesek.xlsx', index=False)


PODownloader().write_po_items_to_excel()
