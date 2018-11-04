import rpyc
from rpyc.utils.server import ThreadedServer

def primes(lowerLimit, upperLimit):
    primes = []
    for possiblePrime in range(lowerLimit, upperLimit + 1):
        # Assume number is prime until shown it is not
        isPrime = True
        for num in range(2, int(possiblePrime ** 0.5) + 1):
            if possiblePrime % num == 0:
                isPrime = False
                break
        if isPrime:
            primes.append(possiblePrime)
    return primes

class MyService(rpyc.Service):
    def on_connect(self, conn):    # runs when a connection is created
        print("\n>>> RPyC service connected ...")
    
    def on_disconnect(self, conn):    # runs when a connection has closed
        print("<<< RPyC service disconnected ...")

    def exposed_get_primes(self, lowerLimit, upperLimit):    #exposed method
        print("Starting computation for primes between {} and {} on server...".format(lowerLimit, upperLimit))
        list_of_primes = primes(lowerLimit, upperLimit)
        print(len(list_of_primes))
        print("Computing finished.")
        return list_of_primes

if __name__ == "__main__":
    t = ThreadedServer(MyService, port=18861)
    t.start()