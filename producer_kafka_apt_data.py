# -------- v1 정상 코드, buildings 등 데이터베이스 테이블 분리하기 전

# import time
# import json
# import random
# from kafka import KafkaProducer

# brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]

# num_households = 840
# start_year = 2019
# end_year = 2019  # 원하는 년도로 설정.

# def generate_energy(month):
#     if month in [12, 1, 2]:  # 겨울
#         power = random.randint(200, 400)
#         water = random.randint(10, 100)
#         heating = random.randint(700, 1500)
#     elif month in [3, 4, 5, 9, 10, 11]:  # 봄, 가을
#         power = random.randint(150, 400)
#         water = random.randint(10, 100)
#         heating = random.randint(300, 900)
#     else:  # 6, 7, 8 - 여름
#         power = random.randint(400, 700)
#         water = random.randint(10, 100)
#         heating = random.randint(100, 400)

#     return power, water, heating

# if __name__ == "__main__":
#     producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=lambda x: json.dumps(x).encode('utf-8'))

#     for household_id in range(1, num_households + 1):
#         for year in range(start_year, end_year + 1):
#             for month in range(1, 13):
#                 power, water, heating = generate_energy(month)

#                 # 데이터 생성
#                 data = {"household_id": household_id, "year": year, "month": month, "power": power,
#                         "water": water, "heating": heating}

#                 # 생성한 데이터를 Kafka topic으로 전송
#                 produced_data = producer.send('energy-data-topic', value=data)
#                 print(f"Produced data: {data}")
#                 time.sleep(0.1) # 0.1초 딜레이

#----------- v2 

import time
import json
import random
from kafka import KafkaProducer

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]

total_buildings = 11
buildings_data = {
        1911: {'units_per_floor': 2, 'floors': 20},
        'default': {'units_per_floor': 4, 'floors': 20}                   
}

start_year = 2019
end_year = 2019  # 원하는 년도로 설정.

def generate_energy(month):
    if month in [12, 1, 2]:  # 겨울
        power = random.randint(200, 400)
        water = random.randint(10, 100)
        heating = random.randint(700, 1500)
    elif month in [3, 4, 5, 9, 10, 11]:  # 봄, 가을
        power = random.randint(150, 400)
        water = random.randint(10, 100)
        heating = random.randint(300, 900)
    else:  # 6, 7, 8 - 여름
        power = random.randint(400, 700)
        water = random.randint(10, 100)
        heating = random.randint(100, 400)

    return power, water, heating

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    household_id = 1
    for building_number in range(1901, 1901 + total_buildings):
        building_config = buildings_data.get(building_number, buildings_data['default'])

        for floor in range(1, building_config['floors'] + 1):
            for unit in range(1, building_config['units_per_floor'] + 1):
                for year in range(start_year, end_year + 1):
                    for month in range(1, 13):
                        power, water, heating = generate_energy(month)

                        # 데이터 생성
                        data = {"household_id": household_id, "building_number": building_number, "floor": floor, "unit": unit, "year": year, "month": month, "power": power,
                                "water": water, "heating": heating}

                        # 생성한 데이터를 Kafka topic으로 전송
                        produced_data = producer.send('energy-data-topic', value=data)
                        print(f"Produced data: {data}")
                        time.sleep(0.5) # 0.1초 딜레이

                household_id += 1
