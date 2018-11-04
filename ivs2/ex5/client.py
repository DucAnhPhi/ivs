import rpyc
import ast
import time
from timeit import default_timer as timer

def callback():
    print("### Finished async request")


if __name__ == "__main__":
    # Parameters here: server-name/IP-adr, server-port
    c = rpyc.connect("localhost", 18861)

    lower, upper = 2, 300000
    parallel_requests = 10
    async_get_primes = rpyc.async_(c.root.get_primes)
    
    print("\n### Starting request ...")
    
    offset = (upper-lower) // parallel_requests
    temp_lower = lower
    list_of_primes = []
    async_results = []

    start = timer()

    for n in range(1, parallel_requests + 1):
        temp_upper = temp_lower + offset
        if n == parallel_requests and temp_upper < upper:
            temp_upper = upper
        result = async_get_primes(temp_lower, temp_upper)
        async_results.append(result)
        temp_lower = temp_upper
    

    while len(async_results) != 0:
        for i in range(len(async_results)):
            if async_results[i].ready:
                callback()
                for prime in async_results[i].value:
                    list_of_primes.append(prime)
                async_results.pop(i)
                break
    
    end = timer()
    
    print("### Received {} primes".format(len(list_of_primes)))
    print("### Computation took {} seconds".format(end-start))