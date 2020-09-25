from queue import Queue
from threading import Thread
from time import time
import psycopg2
import redis
import time
import multiprocessing as mp

def run(records,start_time):
    r = redis.Redis(host="localhost")
    rp = r.pipeline(transaction=False)
    for row in records:
        emplid="Emp"+str(row[0]) 
        rp.hmset(emplid,{"x":"wklfjfejwfojefwiefoiewoiefoweif","y":"kfhjqwleifhewiqofuneqwifbefil","z":"iowjpoiefjpweofijpqwoefijpiofwejoiwe","a":"wklfjfejwfojefwiefoiewoiefoweif","b":"kfhjqwleifhewiqofuneqwifbefil"})
    rp.execute()
    print("---Worker done %s seconds for loop ---" % (time.time() - start_time))


def run_migration():
    print("starting, # of cpus " + str(mp.cpu_count()))
    conn = psycopg2.connect(database="postgres",user = "postgres", password = '', host = "localhost", port = "5432")
    print("connected to PG")
    queue = mp.Queue()
    procs = []
    proc = mp.Process(target=run)
    procs.append(proc)
    port = '6379'    
    chunk_size = 10000
    cur = conn.cursor(name="my_cursor_name")  
    query = "select * from pgbench_accounts"
    cur.execute(query)
    start_time = time.time()
    while True:
        records = cur.fetchmany(size=chunk_size)
        loop_start_time = time.time()
        if not records:
            break
        proc = mp.Process(target=run, args=(records,start_time,))
        procs.append(proc)
        proc.start()
    cur.close()
    conn.close()

    print("---Script done,  %s seconds ---" % (time.time() - start_time))
    
run_migration()
