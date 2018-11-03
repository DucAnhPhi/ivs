def callback(batch_size, timePassed):
    print("{} primes calculated in {} seconds".format(batch_size, timePassed))

if __name__ == "__main__":
    import rpyc
    # Parameters here: server-name/IP-adr, server-port
    c = rpyc.connect("localhost", 18861)

    lower, upper = 2, 3000000
    batch_size = 100000
    print("\n### Starting request ...")

    list_of_primes = c.root.get_primes(lower, upper, batch_size, callback)
    print("### Received {} primes".format(len(list_of_primes)))