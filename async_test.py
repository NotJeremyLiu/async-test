import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
import asyncpg
import subprocess
import json
from timeit import default_timer as timer
from tracemalloc import start
import httpx
from sqlmodel import Field, SQLModel, Column, JSON
from sqlalchemy.orm import sessionmaker

class Block(SQLModel, table=True):
    block_height: int = Field(primary_key=True)
    #block_json: dict = Field(sa_column=Column(JSON))    # Need to make defaul NULL
    txs_json: dict = Field(sa_column=Column(JSON))      # Need to make defaul NULL


'''
response = httpx.post("https://osmosis-mainnet-archive.allthatnode.com:26657/", json = {
        "method": "tx_search",
        "params": [
            "tx.height=5393309",
            False,
            "1",
            "100",
            "asc"
        ],
        "id": 1
    }, 
    headers = {
        "Content-Type": "application/json"
    })
'''

async def block_response(block_height: int) -> dict:

    json_data = {       
        "method": "tx_search",
        "params": [
            f"tx.height={block_height}",
            False,
            "1",
            "100",
            "asc"
        ],
        "id": 1}
    
    headers = {"Content-Type": "application/json"}

    async with httpx.AsyncClient() as client:
        r = await client.post('https://osmosis-mainnet-archive.allthatnode.com:26657/', json=json_data, headers = headers, timeout=None)

    engine = create_async_engine("postgresql+asyncpg://jeremy@localhost:5444/async")

    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    new_block = Block(block_height=block_height, txs_json=r.json())
    '''
    async with async_session() as session:
        session.add(new_block)
        await session.commit()


        #print(r.json())
    '''
'''
async def check_output(*args):

    p = await asyncio.create_subprocess_exec(
        *args, 
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )   
    stdout_data, stderr_data = await p.communicate()
    if p.returncode == 0:
        return json.loads(stdout_data)

async def query_block(block_height: int) -> dict:
    #print(f"querying block: {block_height}")
    block = await check_output(*["/Users/jeremy/osmosis/build/osmosisd", "query", "block", str(block_height)])
    return block
'''

def query_blocks(start_block_height: int, end_block_height: int) -> None:
    task_list = []
    for block_height in range(start_block_height, end_block_height):
        #task_list.append(query_block(block_height=block_height))
        task_list.append(block_response(block_height=block_height))
    loop = asyncio.get_event_loop()
    reslt = loop.run_until_complete(asyncio.gather(*task_list))
    #print(reslt)
    #print(type(reslt))


start_time = timer()

beginning_block_height = 5403000
block_height = 5403000
end_block_height = 5404000
chunk = 200

while block_height < end_block_height:
    #print(block_height)
    query_blocks(start_block_height=block_height, end_block_height=block_height+chunk)     
    block_height += chunk

#query_blocks(start_block_height=block_height, end_block_height=end_block_height) 


end_time = timer()
print(f"Querying and storing {end_block_height - beginning_block_height} blocks in db took {end_time - start_time} seconds")



'''
async def run_throttled(input, sem):
    async with sem:
        result = await run_program(input)
    return result

LIMIT = 10

async def many_programs(inputs):
    sem = asyncio.Semaphore(LIMIT)
    results = await asyncio.gather(
        *[run_throttled(input, sem) for input in inputs])
    # ...
'''