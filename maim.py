"""
Программа для управления клиентами с использованием базы данных PostgreSQL
"""

import psycopg2
from psycopg2 import Error
from typing import List, Optional, Dict, Any


class ClientDatabase:
    """Класс для работы с базой данных клиентов"""

    def __init__(self, dbname: str, user: str, password: str, host: str = "localhost", port: str = "5432"):
        """
        Инициализация подключения к базе данных

        Args:
            dbname: название базы данных
            user: имя пользователя
            password: пароль
            host: хост (по умолчанию localhost)
            port: порт (по умолчанию 5432)
        """
        self.connection = None
        try:
            print(f"Подключаюсь к базе данных {dbname}...")
            self.connection = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print("✓ Подключение успешно установлено")
        except Error as e:
            print(f"✗ Ошибка при подключении: {e}")
            print("\nСОВЕТ: Убедитесь что:")
            print("1. PostgreSQL установлен и запущен")
            print("2. База данных существует")
            print("3. Правильные логин/пароль")
            raise

    def create_tables(self) -> None:
        """
        Функция 1: Создает структуру БД (таблицы)

        Создает две таблицы:
        - clients: для хранения информации о клиентах
        - phones: для хранения телефонов клиентов (один ко многим)
        """
        print("\n" + "=" * 60)
        print("СОЗДАНИЕ СТРУКТУРЫ БАЗЫ ДАННЫХ")
        print("=" * 60)

        try:
            # 1. Таблица клиентов
            print("Создаю таблицу 'clients'...")
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS clients (
                    client_id SERIAL PRIMARY KEY,
                    first_name VARCHAR(50) NOT NULL,
                    last_name VARCHAR(50) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            print("✓ Таблица 'clients' создана")

            # 2. Таблица телефонов
            print("Создаю таблицу 'phones'...")
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS phones (
                    phone_id SERIAL PRIMARY KEY,
                    client_id INTEGER NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
                    phone_number VARCHAR(20) UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            print("✓ Таблица 'phones' создана")

            print("\n✅ Структура базы данных успешно создана!")
            print("\nСозданные таблицы:")
            print("1. clients - информация о клиентах")
            print("2. phones - телефоны клиентов (связь один-ко-многим)")

        except Error as e:
            print(f"✗ Ошибка при создании таблиц: {e}")

    def add_client(self, first_name: str, last_name: str, email: str, phones: Optional[List[str]] = None) -> int:
        """
        Функция 2: Добавляет нового клиента

        Args:
            first_name: имя клиента
            last_name: фамилия клиента
            email: email клиента (уникальный)
            phones: список телефонов (опционально)

        Returns:
            ID созданного клиента или -1 при ошибке
        """
        print(f"\n{'=' * 60}")
        print(f"ДОБАВЛЕНИЕ НОВОГО КЛИЕНТА")
        print(f"{'=' * 60}")
        print(f"Имя: {first_name}")
        print(f"Фамилия: {last_name}")
        print(f"Email: {email}")
        if phones:
            print(f"Телефоны: {', '.join(phones)}")

        try:
            # Вставляем данные клиента
            self.cursor.execute("""
                INSERT INTO clients (first_name, last_name, email) 
                VALUES (%s, %s, %s) 
                RETURNING client_id;
            """, (first_name, last_name, email))

            client_id = self.cursor.fetchone()[0]
            print(f"✓ Клиент создан с ID: {client_id}")

            # Если переданы телефоны, добавляем их
            if phones:
                for phone in phones:
                    self.add_phone(client_id, phone)

            return client_id

        except Error as e:
            print(f"✗ Ошибка при добавлении клиента: {e}")
            return -1

    def add_phone(self, client_id: int, phone: str) -> bool:
        """
        Функция 3: Добавляет телефон для существующего клиента

        Args:
            client_id: ID клиента
            phone: номер телефона

        Returns:
            True если успешно, False если ошибка
        """
        print(f"\nДобавляю телефон для клиента ID {client_id}: {phone}")

        try:
            # Проверяем существование клиента
            self.cursor.execute("SELECT client_id FROM clients WHERE client_id = %s", (client_id,))
            if not self.cursor.fetchone():
                print(f"✗ Клиент с ID {client_id} не найден")
                return False

            # Добавляем телефон
            self.cursor.execute("""
                INSERT INTO phones (client_id, phone_number)
                VALUES (%s, %s)
                ON CONFLICT (phone_number) DO NOTHING
                RETURNING phone_id;
            """, (client_id, phone))

            result = self.cursor.fetchone()
            if result:
                print(f"✓ Телефон добавлен (ID: {result[0]})")
                return True
            else:
                print("⚠ Этот телефон уже существует у другого клиента")
                return False

        except Error as e:
            print(f"✗ Ошибка при добавлении телефона: {e}")
            return False

    def update_client(self, client_id: int, **kwargs) -> bool:
        """
        Функция 4: Изменяет данные о клиенте

        Args:
            client_id: ID клиента
            **kwargs: поля для обновления (first_name, last_name, email)

        Returns:
            True если успешно, False если ошибка
        """
        print(f"\n{'=' * 60}")
        print(f"ОБНОВЛЕНИЕ ДАННЫХ КЛИЕНТА ID: {client_id}")
        print(f"{'=' * 60}")

        # Показываем что обновляем
        for key, value in kwargs.items():
            print(f"{key}: {value}")

        try:
            # Проверяем существование клиента
            self.cursor.execute("SELECT client_id FROM clients WHERE client_id = %s", (client_id,))
            if not self.cursor.fetchone():
                print(f"✗ Клиент с ID {client_id} не найден")
                return False

            # Формируем запрос на обновление
            update_fields = []
            values = []

            if 'first_name' in kwargs:
                update_fields.append("first_name = %s")
                values.append(kwargs['first_name'])

            if 'last_name' in kwargs:
                update_fields.append("last_name = %s")
                values.append(kwargs['last_name'])

            if 'email' in kwargs:
                update_fields.append("email = %s")
                values.append(kwargs['email'])

            if not update_fields:
                print("⚠ Не указаны поля для обновления")
                return False

            values.append(client_id)
            query = f"UPDATE clients SET {', '.join(update_fields)} WHERE client_id = %s"

            self.cursor.execute(query, values)
            print(f"✓ Данные клиента обновлены")
            return True

        except Error as e:
            print(f"✗ Ошибка при обновлении клиента: {e}")
            return False

    def delete_phone(self, client_id: int, phone: str) -> bool:
        """
        Функция 5: Удаляет телефон у клиента

        Args:
            client_id: ID клиента
            phone: номер телефона для удаления

        Returns:
            True если успешно, False если ошибка
        """
        print(f"\nУдаляю телефон {phone} у клиента ID {client_id}")

        try:
            # Проверяем существование клиента
            self.cursor.execute("SELECT client_id FROM clients WHERE client_id = %s", (client_id,))
            if not self.cursor.fetchone():
                print(f"✗ Клиент с ID {client_id} не найден")
                return False

            # Удаляем телефон
            self.cursor.execute("""
                DELETE FROM phones 
                WHERE client_id = %s AND phone_number = %s
                RETURNING phone_id;
            """, (client_id, phone))

            result = self.cursor.fetchone()
            if result:
                print(f"✓ Телефон удален (ID телефона: {result[0]})")
                return True
            else:
                print(f"✗ Телефон {phone} не найден у клиента с ID {client_id}")
                return False

        except Error as e:
            print(f"✗ Ошибка при удалении телефона: {e}")
            return False

    def delete_client(self, client_id: int) -> bool:
        """
        Функция 6: Удаляет клиента

        Args:
            client_id: ID клиента для удаления

        Returns:
            True если успешно, False если ошибка
        """
        print(f"\n{'=' * 60}")
        print(f"УДАЛЕНИЕ КЛИЕНТА ID: {client_id}")
        print(f"{'=' * 60}")

        try:
            # Получаем информацию о клиенте перед удалением
            self.cursor.execute("""
                SELECT first_name, last_name FROM clients 
                WHERE client_id = %s
            """, (client_id,))

            client_info = self.cursor.fetchone()
            if not client_info:
                print(f"✗ Клиент с ID {client_id} не найден")
                return False

            print(f"Удаляю клиента: {client_info[0]} {client_info[1]}")

            # Удаляем клиента (телефоны удалятся автоматически благодаря CASCADE)
            self.cursor.execute("DELETE FROM clients WHERE client_id = %s", (client_id,))

            print("✓ Клиент и все его телефоны удалены")
            return True

        except Error as e:
            print(f"✗ Ошибка при удалении клиента: {e}")
            return False

    def find_client(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Функция 7: Находит клиента по данным

        Args:
            **kwargs: параметры поиска (first_name, last_name, email, phone)

        Returns:
            Список найденных клиентов
        """
        print(f"\n{'=' * 60}")
        print("ПОИСК КЛИЕНТА")
        print(f"{'=' * 60}")

        # Показываем параметры поиска
        for key, value in kwargs.items():
            print(f"Поиск по {key}: {value}")

        try:
            conditions = []
            values = []

            # Формируем условия поиска
            if 'first_name' in kwargs:
                conditions.append("c.first_name ILIKE %s")
                values.append(f"%{kwargs['first_name']}%")

            if 'last_name' in kwargs:
                conditions.append("c.last_name ILIKE %s")
                values.append(f"%{kwargs['last_name']}%")

            if 'email' in kwargs:
                conditions.append("c.email ILIKE %s")
                values.append(f"%{kwargs['email']}%")

            if 'phone' in kwargs:
                conditions.append("p.phone_number ILIKE %s")
                values.append(f"%{kwargs['phone']}%")

            if not conditions:
                print("⚠ Не указаны параметры поиска")
                return []

            # Строим запрос
            query = """
                SELECT DISTINCT 
                    c.client_id, 
                    c.first_name, 
                    c.last_name, 
                    c.email,
                    c.created_at,
                    ARRAY_AGG(p.phone_number) FILTER (WHERE p.phone_number IS NOT NULL) as phones
                FROM clients c
                LEFT JOIN phones p ON c.client_id = p.client_id
                WHERE {}
                GROUP BY c.client_id, c.first_name, c.last_name, c.email, c.created_at
                ORDER BY c.client_id;
            """.format(" AND ".join(conditions))

            self.cursor.execute(query, values)
            results = self.cursor.fetchall()

            # Форматируем результаты
            clients = []
            for row in results:
                client = {
                    'client_id': row[0],
                    'first_name': row[1],
                    'last_name': row[2],
                    'email': row[3],
                    'created_at': row[4],
                    'phones': row[5] if row[5] else []
                }
                clients.append(client)

            # Показываем результаты
            if clients:
                print(f"\n✅ Найдено клиентов: {len(clients)}")
                for i, client in enumerate(clients, 1):
                    print(f"\n{i}. {client['first_name']} {client['last_name']}")
                    print(f"   ID: {client['client_id']}")
                    print(f"   Email: {client['email']}")
                    print(f"   Телефоны: {', '.join(client['phones']) if client['phones'] else 'нет телефонов'}")
                    print(f"   Зарегистрирован: {client['created_at'].strftime('%d.%m.%Y %H:%M')}")
            else:
                print("\n❌ Клиенты не найдены")

            return clients

        except Error as e:
            print(f"✗ Ошибка при поиске клиента: {e}")
            return []

    # ========== ДОПОЛНИТЕЛЬНЫЕ ФУНКЦИИ ==========

    def get_client_info(self, client_id: int) -> Optional[Dict[str, Any]]:
        """Получает полную информацию о клиенте по ID"""
        try:
            self.cursor.execute("""
                SELECT 
                    c.client_id, 
                    c.first_name, 
                    c.last_name, 
                    c.email,
                    c.created_at,
                    ARRAY_AGG(p.phone_number) as phones
                FROM clients c
                LEFT JOIN phones p ON c.client_id = p.client_id
                WHERE c.client_id = %s
                GROUP BY c.client_id, c.first_name, c.last_name, c.email, c.created_at;
            """, (client_id,))

            result = self.cursor.fetchone()
            if result:
                return {
                    'client_id': result[0],
                    'first_name': result[1],
                    'last_name': result[2],
                    'email': result[3],
                    'created_at': result[4],
                    'phones': result[5] if result[5] else []
                }
            return None

        except Error as e:
            print(f"✗ Ошибка: {e}")
            return None

    def get_all_clients(self) -> List[Dict[str, Any]]:
        """Получает список всех клиентов"""
        try:
            self.cursor.execute("""
                SELECT 
                    c.client_id, 
                    c.first_name, 
                    c.last_name, 
                    c.email,
                    c.created_at,
                    ARRAY_AGG(p.phone_number) as phones
                FROM clients c
                LEFT JOIN phones p ON c.client_id = p.client_id
                GROUP BY c.client_id, c.first_name, c.last_name, c.email, c.created_at
                ORDER BY c.client_id;
            """)

            results = self.cursor.fetchall()
            clients = []
            for row in results:
                client = {
                    'client_id': row[0],
                    'first_name': row[1],
                    'last_name': row[2],
                    'email': row[3],
                    'created_at': row[4],
                    'phones': row[5] if row[5] else []
                }
                clients.append(client)

            return clients

        except Error as e:
            print(f"✗ Ошибка: {e}")
            return []

    def show_all_clients(self) -> None:
        """Показывает всех клиентов в удобном формате"""
        clients = self.get_all_clients()

        print(f"\n{'=' * 60}")
        print(f"ВСЕ КЛИЕНТЫ В БАЗЕ (всего: {len(clients)})")
        print(f"{'=' * 60}")

        if not clients:
            print("В базе нет клиентов")
            return

        for i, client in enumerate(clients, 1):
            print(f"\n{i}. {client['first_name']} {client['last_name']}")
            print(f"   ID: {client['client_id']}")
            print(f"   Email: {client['email']}")
            print(f"   Телефоны: {', '.join(client['phones']) if client['phones'] else 'нет телефонов'}")
            print(f"   Зарегистрирован: {client['created_at'].strftime('%d.%m.%Y %H:%M')}")

    def close_connection(self) -> None:
        """Закрывает подключение к базе данных"""
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("\n✓ Подключение к базе данных закрыто")


def demonstrate_functions():
    """
    Демонстрация работы всех функций
    Эта функция запускается автоматически при запуске программы
    """
    print("\n" + "=" * 60)
    print("ПРОГРАММА ДЛЯ УПРАВЛЕНИЯ КЛИЕНТАМИ")
    print("=" * 60)

    # ========== НАСТРОЙКИ ПОДКЛЮЧЕНИЯ ==========
    # ИЗМЕНИТЕ ЭТИ НАСТРОЙКИ ПОД СВОЙ КОМПЬЮТЕР!

    DB_CONFIG = {
        'dbname': 'clients_db',  # Название вашей базы данных
        'user': 'postgres',  # Ваш пользователь PostgreSQL
        'password': 'volhin21',  # Ваш пароль PostgreSQL
        'host': 'localhost',  # Обычно localhost
        'port': '5432'  # Обычно 5432
    }

    # ========== СОЗДАНИЕ БАЗЫ ДАННЫХ ==========
    print("\n📝 ПРЕДВАРИТЕЛЬНАЯ ПОДГОТОВКА:")
    print("1. Установите PostgreSQL (если не установлен)")
    print("2. Создайте базу данных командой:")
    print("   CREATE DATABASE clients_db;")
    print("3. Установите библиотеку psycopg2:")
    print("   pip install psycopg2-binary")
    print("\nЕсли база данных уже создана - нажмите Enter...")
    input()

    # ========== ПОДКЛЮЧЕНИЕ К БАЗЕ ==========
    try:
        print("\n🔗 ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ...")
        db = ClientDatabase(**DB_CONFIG)
    except Exception as e:
        print(f"\n❌ Не удалось подключиться: {e}")
        print("\nПроверьте:")
        print("1. Запущен ли PostgreSQL?")
        print("2. Правильные ли логин/пароль?")
        print("3. Существует ли база данных 'clients_db'?")
        return

    # ========== ДЕМОНСТРАЦИЯ ВСЕХ ФУНКЦИЙ ==========

    print("\n" + "=" * 60)
    print("НАЧАЛО ДЕМОНСТРАЦИИ")
    print("=" * 60)

    # ФУНКЦИЯ 1: Создание таблиц
    db.create_tables()

    # ФУНКЦИЯ 2: Добавление клиентов
    print("\n📝 ДОБАВЛЯЕМ КЛИЕНТОВ:")

    client1_id = db.add_client(
        first_name="Иван",
        last_name="Иванов",
        email="ivanov@example.com",
        phones=["+79161234567", "+74951234567"]
    )

    client2_id = db.add_client(
        first_name="Петр",
        last_name="Петров",
        email="petrov@example.com",
        phones=["+79169876543"]
    )

    client3_id = db.add_client(
        first_name="Мария",
        last_name="Сидорова",
        email="sidorova@example.com"
        # Без телефона - клиент может не иметь телефона!
    )

    # ФУНКЦИЯ 3: Добавление телефонов
    print("\n📱 ДОБАВЛЯЕМ ТЕЛЕФОНЫ:")
    db.add_phone(client3_id, "+79167778899")  # Добавляем телефон Марии
    db.add_phone(client1_id, "+79031112233")  # Добавляем третий телефон Ивану

    # ФУНКЦИЯ 7: Поиск клиентов (демонстрация поиска)
    print("\n🔍 ПОИСК КЛИЕНТОВ:")

    print("\n1. Поиск по имени 'Иван':")
    db.find_client(first_name="Иван")

    print("\n2. Поиск по телефону '+7916':")
    db.find_client(phone="+7916")

    print("\n3. Поиск по email 'example':")
    db.find_client(email="example")

    # ФУНКЦИЯ 4: Обновление данных
    print("\n✏️ ОБНОВЛЯЕМ ДАННЫЕ КЛИЕНТА:")
    db.update_client(
        client_id=client2_id,
        first_name="Пётр",  # Изменяем имя
        email="new_petrov@example.com"  # Изменяем email
    )

    # Показываем всех клиентов после обновлений
    db.show_all_clients()

    # ФУНКЦИЯ 5: Удаление телефона
    print("\n🗑️ УДАЛЯЕМ ТЕЛЕФОН:")
    db.delete_phone(client1_id, "+74951234567")

    # ФУНКЦИЯ 6: Удаление клиента
    print("\n🗑️ УДАЛЯЕМ КЛИЕНТА:")
    db.delete_client(client3_id)

    # Показываем финальный результат
    print("\n" + "=" * 60)
    print("ФИНАЛЬНЫЙ РЕЗУЛЬТАТ")
    print("=" * 60)
    db.show_all_clients()

    # Закрываем подключение
    db.close_connection()

    print("\n" + "=" * 60)
    print("✅ ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА УСПЕШНО!")
    print("=" * 60)
    print("\nВсе функции были продемонстрированы:")
    print("1. create_tables() - создание структуры БД ✓")
    print("2. add_client() - добавление клиента ✓")
    print("3. add_phone() - добавление телефона ✓")
    print("4. update_client() - изменение данных ✓")
    print("5. delete_phone() - удаление телефона ✓")
    print("6. delete_client() - удаление клиента ✓")
    print("7. find_client() - поиск клиента ✓")


def simple_test():
    """
    Простой тест без демонстрации
    Для быстрой проверки работы
    """
    print("Простой тест работы программы...")

    # Минимальные настройки
    db = ClientDatabase(
        dbname='clients_db',
        user='postgres',
        password='password'
    )

    # Создаем таблицы
    db.create_tables()

    # Добавляем одного клиента
    client_id = db.add_client(
        first_name="Тест",
        last_name="Тестов",
        email="test@test.com",
        phones=["+79990000000"]
    )

    # Ищем его
    db.find_client(last_name="Тестов")

    # Закрываем
    db.close_connection()


# ========== ТОЧКА ВХОДА ==========
if __name__ == "__main__":
    """
    Это главная функция, которая запускается при запуске программы
    """

    print("""
    ╔══════════════════════════════════════════════════════╗
    ║      ПРОГРАММА ДЛЯ УПРАВЛЕНИЯ КЛИЕНТАМИ             ║
    ║                (Управление БД PostgreSQL)           ║
    ╚══════════════════════════════════════════════════════╝
    """)

    # Меню выбора
    print("\nВыберите режим работы:")
    print("1. Полная демонстрация всех функций (рекомендуется)")
    print("2. Простой тест")
    print("3. Выйти")

    choice = input("\nВведите номер (1-3): ").strip()

    if choice == "1":
        demonstrate_functions()
    elif choice == "2":
        simple_test()
    elif choice == "3":
        print("Выход...")
    else:
        print("Неверный выбор. Запускаю демонстрацию...")
        demonstrate_functions()

    input("\nНажмите Enter для выхода...")