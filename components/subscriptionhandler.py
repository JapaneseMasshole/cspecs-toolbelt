import blpapi as bp
import json
from pprint import pprint
import time
import threading
from db.database import Database
import datetime

class SubscriptionHandler():
    def __init__(self, config_path):

        self._session = None
        self._sessionOptions = None
        self._subscriptions = []

        with open(config_path, 'r') as f:
            self._config = json.load(f)
        pprint(self._config)

        self._sessionOptions = bp.SessionOptions()
        for i,host in enumerate(self._config['hosts']):
            self._sessionOptions.setServerAddress(host['addr'],host['port'],i)
        
        authOpts = bp.AuthOptions.createWithApp(appName=self._config['appname'])
        self._sessionOptions.setSessionIdentityOptions(authOpts)

        if "tlsInfo" in self._config:
            tlsInfo = self._config["tlsInfo"]
            pk12Blob = None
            pk7Blob = None
            with open(tlsInfo['pk12path'],'rb') as pk12File:
                pk12Blob = pk12File.read()
            with open(tlsInfo['pk7path'],'rb') as pk7File:
                pk7Blob = pk7File.read()

            self._sessionOptions.setTlsOptions(bp.TlsOptions.createFromBlobs(pk12Blob, tlsInfo['password'], pk7Blob))

        self._session = bp.Session(self._sessionOptions, self.eventHandler)
        self._session.startAsync()

        self.db = Database('./db/tickcapturejobs.db')
        self.active_subscriptions = {}
        self.stop_event = threading.Event()
        self.job_cache = {}
        self.last_cache_update = 0
        self.cache_update_interval = 60  # Update cache every 60 seconds
        self.subscription_thread = threading.Thread(target=self.manage_subscriptions)
        self.subscription_thread.start()

    def eventHandler(self,event,session):
        for msg in event:
            print(msg)
            # Here you can add logic to process and store the subscription data

    def start(self):
        self._session.start()

    def stop(self):
        self.stop_event.set()
        self.subscription_thread.join()
        self.__del__()

    def manage_subscriptions(self):
        while not self.stop_event.is_set():
            current_time = time.time()
            
            self.check_for_updates()

            if current_time - self.last_cache_update > self.cache_update_interval:
                self.update_job_cache(current_time)

            # Process jobs from cache
            for job_id, job in self.job_cache.items():
                if job_id not in self.active_subscriptions:
                    self.start_subscription(job)

            # Stop expired subscriptions
            for job_id in list(self.active_subscriptions.keys()):
                if job_id not in self.job_cache:
                    self.stop_subscription(job_id)

            time.sleep(5)  # Still sleep, but now we're using cached data

    def update_job_cache(self, current_time):
        jobs = self.db.query_active_jobs(current_time)
        self.job_cache = {job['id']: job for job in jobs}
        self.last_cache_update = current_time

    def start_subscription(self, job):
        instruments = json.loads(job['instruments'])
        fields = json.loads(job['fields'])
        
        subscriptions = []
        for instrument in instruments:
            sub = bp.SubscriptionList()
            sub.add(instrument, fields)
            subscriptions.append(sub)

        self._session.subscribe(subscriptions)
        self.active_subscriptions[job['id']] = subscriptions
        print(f"Started subscription for job {job['id']}")

    def stop_subscription(self, job_id):
        if job_id in self.active_subscriptions:
            self._session.unsubscribe(self.active_subscriptions[job_id])
            del self.active_subscriptions[job_id]
            print(f"Stopped subscription for job {job_id}")

    def check_for_updates(self):
        if self.db.check_update_flag():
            self.update_job_cache(time.time())
            self.db.clear_update_flag()

    def __del__(self):
        self._session.stopAsync()

if __name__ == "__main__":
    handler = SubscriptionHandler("config/bpipe_config.json")
    handler.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        handler.stop()
