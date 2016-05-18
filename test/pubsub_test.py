import pydis

def subscrube_test():
        client = pydis.Pydis('192.168.99.100', 32768)
        pub = client.pubsub()
        pub.subscribe('mychannel')
        for x in range(5):
            msg = pub.listen()
            print(msg)

subscrube_test()
