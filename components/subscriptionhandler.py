import blpapi as bp
import json
from pprint import pprint,pformat
import time
import threading
from db.database import Database
import datetime
from typing import Dict, List, Any, Optional
from components.logger_config import get_logger
from models.subscriptionitem import SubscriptionItem

logger = get_logger(__name__)

class SubscriptionHandler:
    _instance: Optional['SubscriptionHandler'] = None
    _lock = threading.Lock()

    def __new__(cls, config_path: str) -> 'SubscriptionHandler':
        with cls._lock:
            if cls._instance is None:
                logger.info("Creating new SubscriptionHandler instance")
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, config_path: str):
        if self._initialized:
            logger.debug("SubscriptionHandler already initialized, skipping initialization")
            return
            
        logger.info(f"Initializing SubscriptionHandler with config: {config_path}")

        self._session = None
        self._sessionOptions = None
        self._subscription_list = bp.SubscriptionList()  # Single SubscriptionList instance
        self._active_instruments: Dict[str, SubscriptionItem] = {}  # Track active instruments

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
            self.active_subscriptions: Dict[int, List[str]] = {}  # Store job_id -> list of instruments
            self.stop_event: threading.Event = threading.Event()
            self.job_cache: Dict[int, Dict[str, Any]] = {}
            self.last_cache_update: float = 0
            self.cache_update_interval: int = 60
            self.subscription_thread: threading.Thread = threading.Thread(
                target=self.manage_subscriptions,
                name="SubscriptionManagerThread"
            )
            self.subscription_thread.daemon = True
            self.subscription_thread.start()

            self._initialized = True
            logger.info("SubscriptionHandler initialization completed")

        except Exception as e:
            logger.error(f"Error initializing SubscriptionHandler: {str(e)}", exc_info=True)
            raise

    def start_subscription(self, job: Dict[str, Any]) -> None:
        logger.debug(f"Starting subscription for job {job['id']} with instruments: {job['instruments']} and fields: {job['fields']}")
        
        try:
            job_instruments = []
            for instrument in job['instruments']:
                # Create SubscriptionItem for correlation
                sub_item = SubscriptionItem(instrument=instrument, jobid=job['id'])
                
                # Only add to SubscriptionList if not already subscribed
                if instrument not in self._active_instruments:
                    self._subscription_list.add(
                        topic=instrument,
                        fields=job['fields'],
                        correlationId=bp.CorrelationId(sub_item)
                    )
                    self._active_instruments[instrument] = sub_item
                
                job_instruments.append(instrument)

            # If we added any new instruments, subscribe
            if job_instruments:
                self._session.subscribe(self._subscription_list)
            
            # Store the job's instruments
            self.active_subscriptions[job['id']] = job_instruments
            logger.debug(f"Started subscription for job {job['id']}")
            
        except Exception as e:
            logger.error(f"Error starting subscription for job {job['id']}: {str(e)}", exc_info=True)
            raise

    def stop_subscription(self, job_id: int) -> None:
        if job_id in self.active_subscriptions:
            logger.info(f"Stopping subscription for job {job_id}")
            try:
                # Get instruments for this job
                instruments = self.active_subscriptions[job_id]
                
                # Create a list for instruments to unsubscribe
                to_unsubscribe = []
                
                for instrument in instruments:
                    # Check if instrument is used by other jobs
                    used_by_others = any(
                        instrument in job_instruments 
                        for jid, job_instruments in self.active_subscriptions.items() 
                        if jid != job_id
                    )
                    
                    if not used_by_others:
                        # If instrument is not used by other jobs, unsubscribe
                        to_unsubscribe.append(self._active_instruments[instrument])
                        del self._active_instruments[instrument]

                # If we have instruments to unsubscribe, create a new SubscriptionList for them
                if to_unsubscribe:
                    unsub_list = bp.SubscriptionList()
                    for sub_item in to_unsubscribe:
                        unsub_list.add(
                            security=sub_item.instrument,
                            correlationId=bp.CorrelationId(sub_item)
                        )
                    self._session.unsubscribe(unsub_list)

                # Remove job from active subscriptions
                del self.active_subscriptions[job_id]
                logger.debug(f"Stopped subscription for job {job_id}")

            except Exception as e:
                logger.error(f"Error stopping subscription for job {job_id}: {str(e)}", exc_info=True)
                raise

    def eventHandler(self, event: bp.Event, session: bp.Session) -> None:
        try:
            event_type = event.eventType()
            
            if event_type == bp.Event.SUBSCRIPTION_DATA:
                for msg in event:
                    correlation_id = msg.correlationId()
                    if correlation_id:
                        sub_item = correlation_id.value()
                        if isinstance(sub_item, SubscriptionItem):
                            logger.debug(f"Received data for job {sub_item.jobid}, instrument {sub_item.instrument}: {msg}")
                            # Here you can process the subscription data
                            for field in msg.asElement().elements():
                                logger.info(f"Field: {field.name()} = {field.getValueAsString()}")
                        else:
                            logger.warning(f"Unexpected correlation ID type: {type(sub_item)}")
                    else:
                        logger.debug(f"Received message without correlation ID: {msg}")
            
            elif event_type == bp.Event.SUBSCRIPTION_STATUS:
                for msg in event:
                    correlation_id = msg.correlationId()
                    if correlation_id:
                        sub_item = correlation_id.value()
                        if isinstance(sub_item, SubscriptionItem):
                            logger.info(f"Subscription status for job {sub_item.jobid}, instrument {sub_item.instrument}: {msg}")
                        else:
                            logger.warning(f"Unexpected correlation ID type: {type(sub_item)}")
            
            elif event_type == bp.Event.SESSION_STATUS:
                for msg in event:
                    logger.info(f"Session status: {msg}")
            
            else:
                logger.debug(f"Received other event type: {event_type}")
                for msg in event:
                    logger.debug(f"Message: {msg}")

        except Exception as e:
            logger.error(f"Error in eventHandler: {str(e)}", exc_info=True)

    def start(self) -> None:
        logger.info("Starting SubscriptionHandler session")
        self._session.start()

    def stop(self) -> None:
        logger.info("Stopping SubscriptionHandler")
        try:
            self.stop_event.set()
            if self.subscription_thread.is_alive():
                self.subscription_thread.join()
            self.db.close()  # Close the database connection
            if self._session:
                self._session.stopAsync()
        except Exception as e:
            logger.error(f"Error stopping SubscriptionHandler: {str(e)}", exc_info=True)

    def manage_subscriptions(self) -> None:
        logger.info("Starting subscription management")
        while not self.stop_event.is_set():
            current_time: float = time.time()
            logger.debug(f"Current time: {current_time}")
            
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
                    logger.info(f"Job {job_id} no longer in cache, stopping subscription")
                    self.stop_subscription(job_id)

            time.sleep(5)  # Still sleep, but now we're using cached data

    def update_job_cache(self, current_time: float) -> None:
        logger.debug(f"Updating job cache at {current_time}")
        jobs: List[Dict[str, Any]] = self.db.query_active_jobs(current_time)
        logger.debug(f"Jobs: {jobs}")
        self.job_cache = {job['id']: job for job in jobs}
        self.last_cache_update = current_time

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
