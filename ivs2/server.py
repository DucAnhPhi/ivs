import rpyc
from rpyc.utils.server import ThreadedServer
from timeit import default_timer as timer

def primes(lowerLimit, upperLimit, batch_size, callback):
    primes = []
    start = timer()
    for possiblePrime in range(lowerLimit, upperLimit + 1):
        # Assume number is prime until shown it is not
        isPrime = True
        for num in range(2, int(possiblePrime ** 0.5) + 1):
            if possiblePrime % num == 0:
                isPrime = False
                break
        if isPrime:
            primes.append(possiblePrime)
            if len(primes) % batch_size == 0:
                end = timer()
                callback(batch_size, end - start)
                start = timer()
    return primes

class MyService(rpyc.Service):
    def on_connect(self, conn):    # runs when a connection is created
        print("\n>>> RPyC service connected ...")
    
    def on_disconnect(self, conn):    # runs when a connection has closed
        print("<<< RPyC service disconnected ...")

    def exposed_get_primes(self, lowerLimit, upperLimit, batch_size, callback):    #exposed method
        print("Starting computation on server...")
        list_of_primes = primes(lowerLimit, upperLimit, batch_size, callback)
        print("Computing finished.")
        return list_of_primes

if __name__ == "__main__":
    t = ThreadedServer(MyService, port=18861)
    t.start()