import blpapi as bp
import json
from pprint import pprint,pformat
import time
import threading
from db.database import Database
import datetime
from typing import Dict, List, Any
from components.logger_config import get_logger

logger = get_logger(__name__)

class SubscriptionHandler():
    def __init__(self, config_path: str):
        logger.info(f"Initializing SubscriptionHandler with config: {config_path}")

        self._session = None
        self._sessionOptions = None
        self._subscriptions = []

        try:
            with open(config_path, 'r') as f:
                self._config = json.load(f)
            logger.debug(f"Loaded configuration: {pformat(self._config)}")

            self._sessionOptions = bp.SessionOptions()
            for i, host in enumerate(self._config['hosts']):
                self._sessionOptions.setServerAddress(host['addr'], host['port'], i)
            
            if 'appname' not in self._config or not self._config['appname']:
                raise ValueError("ApplicationName is required in the configuration")
            
            authOpts = bp.AuthOptions.createWithApp(appName=self._config['appname'])
            self._sessionOptions.setSessionIdentityOptions(authOpts)

            if "tlsInfo" in self._config:
                tlsInfo = self._config["tlsInfo"]
                pk12Blob = None
                pk7Blob = None
                with open(tlsInfo['pk12path'], 'rb') as pk12File:
                    pk12Blob = pk12File.read()
                with open(tlsInfo['pk7path'], 'rb') as pk7File:
                    pk7Blob = pk7File.read()

                self._sessionOptions.setTlsOptions(bp.TlsOptions.createFromBlobs(pk12Blob, tlsInfo['password'], pk7Blob))

            self._session = bp.Session(self._sessionOptions, self.eventHandler)
            self._session.startAsync()

            self.db: Database = Database('./db/tickcapturejobs.db')
            self.active_subscriptions: Dict[int, List[bp.SubscriptionList]] = {}
            self.stop_event: threading.Event = threading.Event()
            self.job_cache: Dict[int, Dict[str, Any]] = {}
            self.last_cache_update: float = 0
            self.cache_update_interval: int = 60  # Update cache every 60 seconds
            self.subscription_thread: threading.Thread = threading.Thread(target=self.manage_subscriptions)
            self.subscription_thread.start()

        except Exception as e:
            logger.error(f"Error initializing SubscriptionHandler: {str(e)}", exc_info=True)
            raise

    def eventHandler(self, event: bp.Event, session: bp.Session) -> None:
        for msg in event:
            logger.debug(f"Received message: {msg}")
            # Here you can add logic to process and store the subscription data

    def start(self) -> None:
        logger.info("Starting SubscriptionHandler session")
        self._session.start()

    def stop(self) -> None:
        logger.info("Stopping SubscriptionHandler")
        self.stop_event.set()
        self.subscription_thread.join()
        self.__del__()

    def manage_subscriptions(self) -> None:
        logger.info("Starting subscription management")
        while not self.stop_event.is_set():
            current_time: float = time.time()
            
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

    def update_job_cache(self, current_time: float) -> None:
        logger.debug(f"Updating job cache at {current_time}")
        jobs: List[Dict[str, Any]] = self.db.query_active_jobs(current_time)
        self.job_cache = {job['id']: job for job in jobs}
        self.last_cache_update = current_time

    def start_subscription(self, job: Dict[str, Any]) -> None:
        
        instruments: List[str] = job['instruments']
        fields: List[str] = job['fields']

        logger.debug(f"Starting subscription for job {job['id']} with instruments: {instruments} and fields: {fields}")
        
        subscriptions: List[bp.SubscriptionList] = []
        for instrument in instruments:
            sub = bp.SubscriptionList()
            sub.add(instrument, fields=fields,correlationId=bp.CorrelationId({"jobid":job['id'],"security":instrument}))
            subscriptions.append(sub)

        self._session.subscribe(subscriptions)
        self.active_subscriptions[job['id']] = subscriptions
        logger.debug(f"Started subscription for job {job['id']}")

    def stop_subscription(self, job_id: int) -> None:
        if job_id in self.active_subscriptions:
            logger.info(f"Stopping subscription for job {job_id}")
            self._session.unsubscribe(self.active_subscriptions[job_id])
            del self.active_subscriptions[job_id]
            logger.debug(f"Stopped subscription for job {job_id}")

    def check_for_updates(self) -> None:
        if self.db.check_update_flag():
            logger.info("Update flag detected, refreshing job cache")
            self.update_job_cache(time.time())
            self.db.clear_update_flag()

    def __del__(self) -> None:
        logger.info("Stopping SubscriptionHandler session")
        try:
            if self._session:
                self._session.stopAsync()
        except Exception as e:
            logger.error(f"Error stopping SubscriptionHandler session: {str(e)}", exc_info=True)

if __name__ == "__main__":
    handler = SubscriptionHandler("config/bpipe_config.json")
    handler.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        handler.stop()
