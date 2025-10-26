import asyncio
import aiohttp
from bs4 import BeautifulSoup
import time

import pandas as pd

"""
Атрибуты таблицы:

Позиция  
Статус  
Название спутника 
Модель спутника
Оператор 
Место запуска 
Дата запуска 
"""


satellite = {
    "position" : [],
    "status" : [],
    'name' : [],
    'model' : [],
    'operator' : [],
    "launch_place" : [],
    "launch_data" : []
}



async def get_page_data(session, page):
    headers = {
        'User-Agent': 'MyBot/1.0',
        'Accept': 'application/json',
        'X-Custom-Header': 'value'
    }
    url = r"https://www.satbeams.com/satellites?ysclid=mh7f6ptui4257472567"

    async with session.get(url = url, headers = headers) as response:

        response_text = await response.text()
        soup = BeautifulSoup(response_text, 'html.parser')
        rows = soup.find_all('tr', class_= 'class_tr')[1:628]

        for ind, row in enumerate(rows, start=1):
            cells = row.find_all("td")

            try:
                if not (position := cells[1].text.strip()):
                    position = "Nan"

                if not(status := cells[2].text.strip()):
                    status = "Nan"

                if not(name := cells[3].text.strip()):
                    name = 'Nan'

                if not(model := cells[6].text.strip()):
                    model = "Nan"

                if not(operator := cells[7].text.strip()):
                    operator = 'Nan'

                if not(launch_place := cells[-5].text.strip()):
                    launch_place =  'Nan'

                if not(launch_data := cells[-4].text.strip()):
                    launch_data = 'Nan'
            except AttributeError:
                print(f"На итерации {ind} произошла ошибка")
                continue

            satellite['position'].append(position)
            satellite['status'].append(status)
            satellite['name'].append(name)
            satellite['model'].append(model)
            satellite['operator'].append(operator)
            satellite['launch_place'].append(launch_place)
            satellite['launch_data'].append(launch_data)

    print(f"[INFO] page {page} successfull parsed ")




async def gather_data():
    headers = {
        'User-Agent': 'MyBot/1.0',
        'Accept': 'application/json',
        'X-Custom-Header': 'value'
    }
    url = r"https://www.satbeams.com/satellites?ysclid=mh7f6ptui4257472567"

    async with aiohttp.ClientSession(headers =  headers) as session:
        response = await session.get(url)
        if response.status == 200:
            #page = BeautifulSoup()
            page = 1
            tasks = []
            for i in range(page):
                task = asyncio.create_task(get_page_data(session, i + 1))
                tasks.append(task)

            await asyncio.gather(*tasks)
        elif response.status == 404:
            print("Not found")
            return
        else:
            print(f"Error: {response.status}")


def main():
    start_time = time.time()
    asyncio.run(gather_data())
    end_time = time.time()
    print(f"На парсинг сайта было затрачено {end_time - start_time} секунд")

if __name__ == "__main__":
    main()

    link = "satellite.csv"
    pd.DataFrame(satellite).to_csv(link,index = False)
    print(f'Данные успешно сохранены в {link}')

    print(pd.read_csv(link).head())
