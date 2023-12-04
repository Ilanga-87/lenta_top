from fastapi import FastAPI, HTTPException
from clickhouse_driver import Client

# Создаем приложение
app = FastAPI()

# Указываем данные ClickHouse. На проде, естественно, это должно быть вынесено в отдельный скрытый файл
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_DATABASE = "default"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "123"

# Создаем клиента
client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE,
)


# Указываем FastAPI декоратор, прописываем путь и уточняем, что на выходе мы хотим видеть данные в виде списка
@app.get("/getWords", response_model=list)
async def get_top():
    try:
        query = """
            SELECT lowerUTF8(word) as word, count(*) as count -- Выбираем слова и их количество
            FROM (
                -- Разбиваем текст на слова и преобразуем в массив, удаляя лишние знаки препинания, срощенные со словами
                SELECT arrayJoin(splitByChar(' ', replaceRegexpAll(
                lowerUTF8(CONCAT(title, ' ', text, ' ', topic, ' ', tags)), 
                '[.,:;-_]', ''))) 
                as word 
                FROM news
            )
            -- Отфильтровываем пустые слова и отдельно повисшие дефис, тире или подчеркивание
            WHERE word != '' AND NOT match(word, '[-—_]') 
            GROUP BY word -- Группируем по словам
            ORDER BY count DESC LIMIT 100 -- Сортируем по количеству в убывающем порядке и выбираем топ 100
        """
        result = client.execute(query)
        # Отображаем результат в виде списка словарей, где ключом служит слово, а значением - число его вхождений
        top_words = [{row[0]: row[1]} for row in result]

        return top_words

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=9090, log_level="info")
