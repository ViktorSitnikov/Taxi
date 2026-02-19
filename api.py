# main.py
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum

# ---------- Конфигурация БД ----------
DATABASE = "taxi_park.db"

@contextmanager
def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Таблица автомобилей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cars (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT NOT NULL CHECK(status IN ('FREE', 'BUSY', 'REPAIR')) DEFAULT 'FREE',
                license_plate TEXT UNIQUE NOT NULL,
                brand TEXT NOT NULL,
                color TEXT NOT NULL,
                distance REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица водителей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS drivers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT NOT NULL,
                phone TEXT UNIQUE NOT NULL,
                rating REAL DEFAULT 5.0 CHECK(rating >= 0 AND rating <= 5),
                car_id INTEGER UNIQUE,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (car_id) REFERENCES cars (id) ON DELETE SET NULL
            )
        ''')
        
        # Индексы для ускорения
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cars_status ON cars(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_drivers_rating ON drivers(rating)')
        
        # Добавляем тестовые данные
        cursor.execute("SELECT COUNT(*) as cnt FROM cars")
        if cursor.fetchone()['cnt'] == 0:
            cars = [
                ('FREE',  'А123БВ777', 'Toyota',  'Белый',    5.3),
                ('BUSY',  'В456ГД777', 'Hyundai', 'Черный',   2.1),
                ('REPAIR','Е789ЖЗ777', 'Kia',     'Серый',    8.7),
                ('BUSY',  'И012КЛ777', 'Skoda',   'Синий',    1.5),
                ('FREE',  'М345НО777', 'Volkswagen','Красный',4.2),
                ('REPAIR','П678РС777', 'Renault', 'Белый',    6.8),
                ('FREE',  'Т901УФ777', 'Toyota',  'Серебристый',3.4),
            ]
            cursor.executemany(
                'INSERT INTO cars (status, license_plate, brand, color, distance) VALUES (?,?,?,?,?)',
                cars
            )
            
            drivers = [
                ('Иванов Иван Иванович',    '+79001234567', 4.9, 1),
                ('Петров Петр Петрович',    '+79007654321', 4.7, 2),
                ('Сидоров Сидор Сидорович', '+79005554433', 5.0, 4),
                ('Смирнов Алексей Владимирович','+79004443322',4.5,5),
                ('Козлов Дмитрий Николаевич','+79003332211',4.8,7),
            ]
            cursor.executemany(
                'INSERT INTO drivers (full_name, phone, rating, car_id) VALUES (?,?,?,?)',
                drivers
            )

# ---------- Перечисления и Pydantic схемы ----------
class CarStatus(str, Enum):
    FREE = "FREE"
    BUSY = "BUSY"
    REPAIR = "REPAIR"

# Автомобиль (базовая модель)
class CarBase(BaseModel):
    license_plate: str
    brand: str
    color: str
    distance: float = Field(..., gt=0)

class CarCreate(CarBase):
    status: CarStatus = CarStatus.FREE

class CarUpdate(BaseModel):
    status: Optional[CarStatus] = None
    color: Optional[str] = None
    distance: Optional[float] = Field(None, gt=0)

class CarOut(CarBase):
    id: int
    status: CarStatus
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)

class CarWithDriver(CarOut):
    driver: Optional["DriverOut"] = None

# Водитель
class DriverBase(BaseModel):
    full_name: str
    phone: str
    rating: float = Field(5.0, ge=0, le=5)

class DriverCreate(DriverBase):
    car_id: Optional[int] = None

class DriverUpdate(BaseModel):
    full_name: Optional[str] = None
    phone: Optional[str] = None
    rating: Optional[float] = Field(None, ge=0, le=5)
    car_id: Optional[int] = None
    is_active: Optional[bool] = None

class DriverOut(DriverBase):
    id: int
    car_id: Optional[int]
    is_active: bool
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)

class DriverWithCar(DriverOut):
    car: Optional[CarOut] = None

# Для циклических ссылок
CarWithDriver.model_rebuild()
DriverWithCar.model_rebuild()

# ---------- Инициализация приложения ----------
app = FastAPI(title="Taxi Park Admin API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешить все источники (для разработки)
    allow_credentials=True,
    allow_methods=["*"],  # Разрешить все методы
    allow_headers=["*"],  # Разрешить все заголовки
)

init_db()

# ---------- Эндпоинты ----------    
# 5. Машины в ремонте (специализированный эндпоинт)
@app.get("/cars/in-repair", response_model=List[CarOut])
def get_cars_in_repair():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM cars WHERE status = ?", ['REPAIR'])
        rows = cursor.fetchall()
        return [CarOut(**dict(row)) for row in rows]

# 2. Детальная информация об автомобиле
@app.get("/cars/{car_id}", response_model=CarWithDriver)
def get_car(car_id: int = Path(..., ge=1)):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                c.*,
                d.id as driver_id,
                d.full_name as driver_name,
                d.phone as driver_phone,
                d.rating as driver_rating,
                d.is_active as driver_is_active,
                d.created_at as driver_created_at
            FROM cars c
            LEFT JOIN drivers d ON d.car_id = c.id
            WHERE c.id = ?
        """, (car_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(404, "Автомобиль не найден")
        
        car = dict(row)
        driver_data = None
        if car['driver_id']:
            driver_data = {
                'id': car['driver_id'],
                'full_name': car['driver_name'],
                'phone': car['driver_phone'],
                'rating': car['driver_rating'],
                'car_id': car['id'],
                'is_active': car['driver_is_active'],
                'created_at': car['driver_created_at']
            }
        for k in ['driver_id', 'driver_name', 'driver_phone', 'driver_rating', 'driver_is_active', 'driver_created_at']:
            car.pop(k, None)
        car_obj = CarWithDriver(**car)
        if driver_data:
            car_obj.driver = DriverOut(**driver_data)
        return car_obj

# 1. Список всех автомобилей (с фильтрацией по статусу)
@app.get("/cars", response_model=List[CarWithDriver])
def get_cars(
    status: Optional[CarStatus] = Query(None, description="Фильтр по статусу"),
    min_distance: Optional[float] = Query(None, ge=0),
    max_distance: Optional[float] = Query(None, ge=0),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    with get_db() as conn:
        cursor = conn.cursor()
        query = """
            SELECT 
                c.*,
                d.id as driver_id,
                d.full_name as driver_name,
                d.phone as driver_phone,
                d.rating as driver_rating,
                d.is_active as driver_is_active,
                d.created_at as driver_created_at
            FROM cars c
            LEFT JOIN drivers d ON d.car_id = c.id
            WHERE 1=1
        """
        params = []
        if status:
            query += " AND c.status = ?"
            params.append(status.value)
        if min_distance is not None:
            query += " AND c.distance >= ?"
            params.append(min_distance)
        if max_distance is not None:
            query += " AND c.distance <= ?"
            params.append(max_distance)
        query += " ORDER BY c.id LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        result = []
        for row in rows:
            car = dict(row)
            driver_data = None
            if car['driver_id']:
                driver_data = {
                    'id': car['driver_id'],
                    'full_name': car['driver_name'],
                    'phone': car['driver_phone'],
                    'rating': car['driver_rating'],
                    'car_id': car['id'],
                    'is_active': car['driver_is_active'],
                    'created_at': car['driver_created_at']
                }
            # убираем служебные поля водителя из car
            for k in ['driver_id', 'driver_name', 'driver_phone', 'driver_rating', 'driver_is_active', 'driver_created_at']:
                car.pop(k, None)
            car_obj = CarWithDriver(**car)
            if driver_data:
                car_obj.driver = DriverOut(**driver_data)
            result.append(car_obj)
        return result

# 3. Добавить новый автомобиль
@app.post("/cars", response_model=CarOut, status_code=201)
def create_car(car: CarCreate):
    with get_db() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO cars (status, license_plate, brand, color, distance)
                VALUES (?, ?, ?, ?, ?)
            """, (car.status.value, car.license_plate, car.brand, car.color, car.distance))
            car_id = cursor.lastrowid
            cursor.execute("SELECT * FROM cars WHERE id = ?", (car_id,))
            new_car = cursor.fetchone()
            return CarOut(**dict(new_car))
        except sqlite3.IntegrityError:
            raise HTTPException(400, "Автомобиль с таким госномером уже существует")

# 4. Обновить информацию об автомобиле (частично)
@app.patch("/cars/{car_id}", response_model=CarOut)
def update_car(car_id: int, car_update: CarUpdate):
    with get_db() as conn:
        cursor = conn.cursor()
        # Проверяем, что машина существует
        cursor.execute("SELECT id FROM cars WHERE id = ?", (car_id,))
        if not cursor.fetchone():
            raise HTTPException(404, "Автомобиль не найден")
        
        fields = []
        params = []
        if car_update.status is not None:
            fields.append("status = ?")
            params.append(car_update.status.value)
        if car_update.color is not None:
            fields.append("color = ?")
            params.append(car_update.color)
        if car_update.distance is not None:
            fields.append("distance = ?")
            params.append(car_update.distance)
        
        if not fields:
            # Ничего не обновляем, возвращаем текущие данные
            cursor.execute("SELECT * FROM cars WHERE id = ?", (car_id,))
            return CarOut(**dict(cursor.fetchone()))
        
        params.append(car_id)
        cursor.execute(f"UPDATE cars SET {', '.join(fields)} WHERE id = ?", params)
        cursor.execute("SELECT * FROM cars WHERE id = ?", (car_id,))
        updated = cursor.fetchone()
        return CarOut(**dict(updated))

# 6. Список всех водителей
@app.get("/drivers", response_model=List[DriverWithCar])
def get_drivers(
    min_rating: Optional[float] = Query(None, ge=0, le=5),
    only_active: bool = Query(True),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    with get_db() as conn:
        cursor = conn.cursor()
        query = """
            SELECT 
                d.*,
                c.id as car_id_fk,
                c.status as car_status,
                c.license_plate as car_license_plate,
                c.brand as car_brand,
                c.color as car_color,
                c.distance as car_distance,
                c.created_at as car_created_at
            FROM drivers d
            LEFT JOIN cars c ON d.car_id = c.id
            WHERE 1=1
        """
        params = []
        if min_rating is not None:
            query += " AND d.rating >= ?"
            params.append(min_rating)
        if only_active:
            query += " AND d.is_active = 1"
        query += " ORDER BY d.id LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        result = []
        for row in rows:
            driver = dict(row)
            car_data = None
            if driver['car_id_fk']:
                car_data = {
                    'id': driver['car_id_fk'],
                    'status': driver['car_status'],
                    'license_plate': driver['car_license_plate'],
                    'brand': driver['car_brand'],
                    'color': driver['car_color'],
                    'distance': driver['car_distance'],
                    'created_at': driver['car_created_at']
                }
            # убираем служебные поля машины
            for k in ['car_id_fk', 'car_status', 'car_license_plate', 'car_brand', 'car_color', 'car_distance', 'car_created_at']:
                driver.pop(k, None)
            driver_obj = DriverWithCar(**driver)
            if car_data:
                driver_obj.car = CarOut(**car_data)
            result.append(driver_obj)
        return result

# 7. Детальная информация о водителе
@app.get("/drivers/{driver_id}", response_model=DriverWithCar)
def get_driver(driver_id: int = Path(..., ge=1)):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                d.*,
                c.id as car_id_fk,
                c.status as car_status,
                c.license_plate as car_license_plate,
                c.brand as car_brand,
                c.color as car_color,
                c.distance as car_distance,
                c.created_at as car_created_at
            FROM drivers d
            LEFT JOIN cars c ON d.car_id = c.id
            WHERE d.id = ?
        """, (driver_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(404, "Водитель не найден")
        
        driver = dict(row)
        car_data = None
        if driver['car_id_fk']:
            car_data = {
                'id': driver['car_id_fk'],
                'status': driver['car_status'],
                'license_plate': driver['car_license_plate'],
                'brand': driver['car_brand'],
                'color': driver['car_color'],
                'distance': driver['car_distance'],
                'created_at': driver['car_created_at']
            }
        for k in ['car_id_fk', 'car_status', 'car_license_plate', 'car_brand', 'car_color', 'car_distance', 'car_created_at']:
            driver.pop(k, None)
        driver_obj = DriverWithCar(**driver)
        if car_data:
            driver_obj.car = CarOut(**car_data)
        return driver_obj

# 8. Добавить нового водителя
@app.post("/drivers", response_model=DriverOut, status_code=201)
def create_driver(driver: DriverCreate):
    with get_db() as conn:
        cursor = conn.cursor()
        # Проверяем, свободен ли указанный автомобиль
        if driver.car_id:
            cursor.execute("SELECT id FROM cars WHERE id = ?", (driver.car_id,))
            if not cursor.fetchone():
                raise HTTPException(400, "Автомобиль с таким id не существует")
            cursor.execute("SELECT id FROM drivers WHERE car_id = ?", (driver.car_id,))
            if cursor.fetchone():
                raise HTTPException(400, "Автомобиль уже назначен другому водителю")
        
        try:
            cursor.execute("""
                INSERT INTO drivers (full_name, phone, rating, car_id)
                VALUES (?, ?, ?, ?)
            """, (driver.full_name, driver.phone, driver.rating, driver.car_id))
            driver_id = cursor.lastrowid
            cursor.execute("SELECT * FROM drivers WHERE id = ?", (driver_id,))
            new_driver = cursor.fetchone()
            return DriverOut(**dict(new_driver))
        except sqlite3.IntegrityError as e:
            if "phone" in str(e):
                raise HTTPException(400, "Водитель с таким номером телефона уже существует")
            raise HTTPException(400, "Ошибка целостности данных")

# 9. Обновить данные водителя
@app.patch("/drivers/{driver_id}", response_model=DriverOut)
def update_driver(driver_id: int, driver_update: DriverUpdate):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM drivers WHERE id = ?", (driver_id,))
        if not cursor.fetchone():
            raise HTTPException(404, "Водитель не найден")
        
        # Если меняется car_id, проверяем, что новый автомобиль существует и свободен
        if driver_update.car_id is not None:
            if driver_update.car_id == 0:  # отвязать машину
                driver_update.car_id = None
            else:
                cursor.execute("SELECT id FROM cars WHERE id = ?", (driver_update.car_id,))
                if not cursor.fetchone():
                    raise HTTPException(400, "Автомобиль не существует")
                cursor.execute("SELECT id FROM drivers WHERE car_id = ? AND id != ?", 
                             (driver_update.car_id, driver_id))
                if cursor.fetchone():
                    raise HTTPException(400, "Автомобиль уже назначен другому водителю")
        
        fields = []
        params = []
        if driver_update.full_name is not None:
            fields.append("full_name = ?")
            params.append(driver_update.full_name)
        if driver_update.phone is not None:
            fields.append("phone = ?")
            params.append(driver_update.phone)
        if driver_update.rating is not None:
            fields.append("rating = ?")
            params.append(driver_update.rating)
        if driver_update.car_id is not None:
            fields.append("car_id = ?")
            params.append(driver_update.car_id)
        if driver_update.is_active is not None:
            fields.append("is_active = ?")
            params.append(1 if driver_update.is_active else 0)
        
        if not fields:
            cursor.execute("SELECT * FROM drivers WHERE id = ?", (driver_id,))
            return DriverOut(**dict(cursor.fetchone()))
        
        params.append(driver_id)
        try:
            cursor.execute(f"UPDATE drivers SET {', '.join(fields)} WHERE id = ?", params)
        except sqlite3.IntegrityError:
            raise HTTPException(400, "Нарушение уникальности (возможно, телефон уже используется)")
        
        cursor.execute("SELECT * FROM drivers WHERE id = ?", (driver_id,))
        updated = cursor.fetchone()
        return DriverOut(**dict(updated))

# 10. Водители с низким рейтингом (нуждаются в регулировке)
@app.get("/drivers/low-rating", response_model=List[DriverOut])
def get_drivers_low_rating(threshold: float = Query(4.0, ge=0, le=5)):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM drivers 
            WHERE rating < ? AND is_active = 1
            ORDER BY rating ASC
        """, (threshold,))
        rows = cursor.fetchall()
        return [DriverOut(**dict(row)) for row in rows]

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="localhost", port=8000)