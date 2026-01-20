from minio import Minio
from minio.error import S3Error
import requests
from tqdm import tqdm
import os
import tempfile



def get_available_remote_files(base_url, filename_template, year):
    """Проверить какие файлы фактически существуют на сайте"""
    available_files = []

    print("🔍 Проверка доступных файлов на сайте...")


    # СТАРАЯ ВЕРСИЯ
    # for month in tqdm(range(1, 13), desc="Проверка месяца"):
    #     filename = filename_template.format(year=year, month=month)
    #     url = f"{base_url}/{filename}"
    #
    #     try:
    #         response = requests.head(url, timeout=10)
    #         if response.status_code == 200:
    #             available_files.append(filename)
    #             print(f"  ✓ {filename} - доступен")
    #         else:
    #             print(f"  ✗ {filename} - недоступен (код: {response.status_code})")
    #
    #     except requests.exceptions.RequestException as e:
    #         print(f"  ✗ {filename} - ошибка: {e}")

    # Имитируем браузер
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    for month in tqdm(range(1, 13), desc="Проверка месяца"):
        filename = filename_template.format(year=year, month=month)
        url = f"{base_url}/{filename}"

        try:
            # Пробуем с разными заголовками
            response = requests.get(url, headers=headers, timeout=10, stream=True)

            # Проверяем статус код
            if response.status_code == 200:
                # Также проверяем содержание ответа - если файл существует,
                # должны получить нормальные заголовки для файла
                content_type = response.headers.get('Content-Type', '')
                content_length = response.headers.get('Content-Length', '0')

                # Парагветные файлы обычно имеют content-type 'application/octet-stream' или подобный
                if int(content_length) > 1000:  # Проверяем что файл не пустой
                    available_files.append(filename)
                    print(f"  ✓ {filename} - доступен ({content_length} bytes)")
                else:
                    print(f"  ⚠ {filename} - маленький размер ({content_length} bytes)")

                response.close()
            else:
                print(f"  ✗ {filename} - код: {response.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"  ✗ {filename} - ошибка: {e}")


    return available_files




def get_local_minio_files(bucket_name, prefix):
    """Получить список файлов в MinIO"""

    # Настройка клиента MinIO
    minio_client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    local_files = []
    try:
        objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
        for obj in objects:
            filename = obj.object_name.replace(f"{prefix}/", "")
            local_files.append(filename)
    except S3Error as e:
        print(f"Ошибка при чтении бакета: {e}")

    print("=" * 30)
    print(f"✅ Файлы в хранилище MinIO: /{bucket_name}/{prefix}:")
    for i in local_files:
        print("    •", i)
    print("=" * 30)

    return local_files


def download_missing_files(bucket_name = 'bronze',
                           prefix = 'nyc-taxi-data',
                           base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data',
                           # filename_template = 'yellow_tripdata_{year}-{month:02d}.parquet',
                           remote_files = [],
                           local_files = [],
                           # execution_year = 2025,
                           **kwargs):
    """Загрузка только отсутствующих файлов в MinIO"""


    print("=" * 50)
    print("✅ Доступные файлы на сайте:")
    for i in remote_files:
        print("    •", i)
    print("=" * 50)

    print(f"✅ Файлы в хранилище MinIO: /{bucket_name}/{prefix}:")
    for i in local_files:
        print("    •", i)
    print("=" * 50)

    print(f"🎯 Обрабатываем:")

    # Настройка клиента MinIO
    minio_client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    # Создаем бакет если нужно
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"✓ Бакет {bucket_name} создан")
    except S3Error as e:
        return [f"✗ Ошибка бакета: {e}"]

    # Находим отсутствующие файлы
    missing_files = list(set(remote_files) - set(local_files))

    # Блок статистики
    print(f"📊 СТАТИСТИКА:")
    print(f"    • Загружено в MinIO: {len(local_files)} файл(ов)")
    print(f"    • Доступно на сайте: {len(remote_files)} файл(ов)")

    # for file in sorted(remote_files):
    #     print(f"     - {file}")

    print()
    if missing_files:
        print(f"• Из них отсутствует в MinIO: {len(missing_files)} файл(ов)")
        for file in sorted(missing_files):
            print(f"     - {file}")

    if not missing_files:
        print("✅ Все доступные файлы уже загружены")
        return {"status": "success", "message": "Все файлы уже загружены", "downloaded_files": []}

    results = []
    downloaded_files = []



    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }


    # Скачиваем только отсутствующие файлы
    for filename in tqdm(missing_files, desc="Загрузка недостающих"):
        url = f"{base_url}/{filename}"

        try:
            # response = requests.get(url, stream=True)
            response = requests.get(url, headers=headers, stream=True)
            response.raise_for_status()

            # Создаем временный файл
            with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
                temp_path = temp_file.name

                # Скачиваем файл на диск
                total_size = int(response.headers.get('content-length', 0))
                for chunk in response.iter_content(chunk_size=8192 * 8):
                    if chunk:
                        temp_file.write(chunk)

            # Получаем реальный размер файла
            file_size = os.path.getsize(temp_path)

            # Загружаем в MinIO
            minio_client.fput_object(
                bucket_name=bucket_name,
                object_name=f"{prefix}/{filename}",
                file_path=temp_path
            )

            # Удаляем временный файл
            os.unlink(temp_path)

            result_msg = f"✓ {filename} ({file_size / (1024 * 1024):.1f} MB)"
            results.append(result_msg)
            downloaded_files.append(filename)
            print(result_msg)

        except Exception as e:
            # Удаляем временный файл в случае ошибки
            if 'temp_path' in locals():
                try:
                    os.unlink(temp_path)
                except:
                    pass
            error_msg = f"✗ {filename}: {e}"
            results.append(error_msg)
            print(error_msg)

    return {
        "status": "success" if downloaded_files else "partial_success",
        "message": f"Загружено {len(downloaded_files)} из {len(missing_files)} файлов",
        "downloaded_files": downloaded_files,
        "details": results
    }
