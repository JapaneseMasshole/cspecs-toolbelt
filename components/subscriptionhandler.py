import blpapi

class SubscriptionHandler():
    def __init__(self):
        self._subscriptions = []
        print(blpapi.version())
