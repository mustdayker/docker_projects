from datetime import datetime

def time_task():

    now = datetime.now()
    print("Текущая дата и время:", now)
    readable_time = f"Форматированная дата и время: {now.strftime('%d %B %Y, %H:%M:%S')}"

    return readable_time


def all_upper(text):
    print("---"*10)
    print(text)
    print(text.upper())
    print("---" * 10)
    return text.upper()