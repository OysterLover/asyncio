import asyncio
import datetime
from aiohttp import ClientSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, JSON, String
from more_itertools import chunked


PG_DSN = 'postgresql+asyncpg://app:secret@127.0.0.1:5431/app'
engine = create_async_engine(PG_DSN)
Base = declarative_base()
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    # json = Column(JSON)
    name = Column(String)
    height = Column(String)
    birth_year = Column(String)
    eye_color = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    mass = Column(String)
    skin_color = Column(String)

    films = Column(String)
    homeworld = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)


CHUNK_SIZE = 10


async def chunked_async(async_iter, size):

    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            if buffer:
                yield buffer
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


async def get_person(people_id: int, session: ClientSession):
    print(f'begin {people_id}')
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data = await response.json()

    json_name = ""
    json_height = ""
    json_birth_year = ""
    json_eye_color = ""
    json_gender = ""
    json_hair_color = ""
    json_mass = ""
    json_skin_color = ""

    simple_columns = [json_name, json_height, json_birth_year, json_eye_color, json_gender, json_hair_color, json_mass, json_skin_color]

    keys_list = ['name', 'height', 'birth_year', 'eye_color', 'gender', 'hair_color', 'mass', 'skin_color']
    for key in keys_list:
        if key in json_data.keys():
            simple_columns[keys_list.index(key)] = json_data[key]
        else:
            pass

    # difficult columns
    # films
    json_films_titles_list = []
    if 'films' in json_data.keys():
        for film in json_data['films']:
            async with session.get(film) as response1:
                json_film_data = await response1.json()
                json_films_titles_list.append(json_film_data['title'])
    else:
        pass

    json_films_titles_str = ", ".join(json_films_titles_list)

    # homeworld
    json_homeworld = ""
    if 'homeworld' in json_data.keys():
        async with session.get(json_data['homeworld']) as response:
            json_homeworld_data = await response.json()
            json_homeworld = json_homeworld_data['name']

    # species
    json_species_names_list = []
    if 'species' in json_data.keys():
        for species in json_data['species']:
            async with session.get(species) as response1:
                json_species_data = await response1.json()
                json_species_names_list.append(json_species_data['name'])
    else:
        pass
    json_species_names_str = ", ".join(json_species_names_list)

    # starships
    json_starships_names_list = []
    if 'starships' in json_data.keys():
        for starship in json_data['starships']:
            async with session.get(starship) as response1:
                json_starships_data = await response1.json()
                json_starships_names_list.append(json_starships_data['name'])
    else:
        pass
    json_starships_names_str = ", ".join(json_starships_names_list)

    # vehicles
    json_vehicles_names_list = []
    if 'vehicles' in json_data.keys():
        for vehicle in json_data['vehicles']:
            async with session.get(vehicle) as response1:
                json_vehicles_data = await response1.json()
                json_vehicles_names_list.append(json_vehicles_data['name'])
    else:
        pass
    json_vehicles_names_str = ", ".join(json_vehicles_names_list)

    print(f'end {people_id}')
    return simple_columns, json_films_titles_str, json_homeworld, json_species_names_str, json_starships_names_str, json_vehicles_names_str


async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, 83), CHUNK_SIZE):
            coroutines = [get_person(people_id=i, session=session) for i in chunk]
            results = await asyncio.gather(*coroutines)
            for item in results:
                yield item


async def insert_people(people_chunk):
    async with Session() as session:
        session.add_all([People(name=item[0][0],
                                height=item[0][1],
                                birth_year=item[0][2],
                                eye_color=item[0][3],
                                gender=item[0][4],
                                hair_color=item[0][5],
                                mass=item[0][6],
                                skin_color=item[0][7],
                                films=item[1],
                                homeworld=item[2],
                                species=item[3],
                                starships=item[4],
                                vehicles=item[5]
                                ) for item in people_chunk])

        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for chunk in chunked_async(get_people(), CHUNK_SIZE):
        asyncio.create_task(insert_people(chunk))

    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)