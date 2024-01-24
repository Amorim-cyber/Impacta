from bs4 import BeautifulSoup
import requests
import locale


def get_values(exchanges,tag,class_):
    values = []

    for exchange in exchanges:
        url = f"https://www.google.com/finance/quote/{exchange}"
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        content = soup.find_all(tag,class_ = class_)[0].text
        values.append(float(content.replace(',','')))

    return values

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

    exchanges = ["BTC-BRL","USD-BRL"]
    tag = "div"
    class_ = "YMlKec fxKbKc"

    v = get_values(exchanges,tag,class_)

    r = calc(v[0],v[1])

    view(format(v[0]),format(v[1]),format(r))