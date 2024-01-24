from bs4 import BeautifulSoup
import requests
import re #regex
import locale

def get_usd_brl():

    url = f"https://www.google.com/finance/quote/USD-BRL"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    main_content = str(soup.find('body').contents)

    processes_filtered = re.findall("\d\.\d\d\d\d",main_content)
    return float(processes_filtered[0])


def get_btc_brl():

    url = f"https://www.google.com/finance/quote/BTC-BRL"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    main_content = str(soup.find('body').contents)

    processes_filtered = re.findall("\d\d\d,\d\d\d\.\d\d",main_content)
    return float(processes_filtered[0].replace(",",""))

def calc(v1,v2):
    return v1 / v2

def format(value):

    locale.setlocale(locale.LC_ALL,"pt_BR.UTF-8")

    return locale.currency(value, grouping=True, symbol=None)

def view(v1,v2,v3):

    print(
        f"""
            Valores extraidos da internet:\n
            Bitcoin em Reais = R$ {v1}\n
            Dolar em Reais = R$ {v2}\n
            \n
            RESULTADO:\n
            Bitcoin em Dolar = $ {v3}

        """
    )

if __name__ == "__main__":

    v = [

        get_btc_brl(),
        get_usd_brl()

    ]

    r = calc(v[0],v[1])

    view(format(v[0]),format(v[1]),format(r))
