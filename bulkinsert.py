from queue import Queue
from threading import Thread
from time import time
import psycopg2
import redis
import time
import multiprocessing as mp

class InsertWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
def run(records):
    r = redis.Redis(host="localhost")
    loop_start_time = time.time()
    rp = r.pipeline(transaction=False)
    for row in records:
        emplid="Emp"+str(row[0])
        rp.hmset(emplid,{"x":"wklfjfejwfojefwiefoiewoiefoweif","y":"kfhjqwleifhewiqofuneqwifbefil","z":"iowjpoiefjpweofijpqwoefijpiofwejoiwe","a":"wklfjfejwfojefwiefoiewoiefoweif","b":"kfhjqwleifhewiqofuneqwifbefil"})
    rp.execute()
    #print("--- %s seconds for loop ---" % (time.time() - loop_start_time))

def lambda_handler(event, context):
    print("starting")
    conn = psycopg2.connect(database="postgres",user = "postgres", password = '', host = "localhost", port = "5432")
    print("connected to PG")
    queue = mp.Queue()
    procs = []
    proc = mp.Process(target=run)
    procs.append(proc)
    port = '6379'
    chunk_size = 1000
    cur = conn.cursor(name="my_cursor_name")
    query = "select * from pgbench_accounts"
    cur.execute(query)
    start_time = time.time()
    while True:
        records = cur.fetchmany(size=chunk_size)
        if not records:
            break
        proc = mp.Process(target=run, args=(records,))
        procs.append(proc)
        proc.start()
    cur.close()
    conn.close()

    print("---Script done,  %s seconds ---" % (time.time() - start_time))

lambda_handler(1,1)
