from dataclasses import dataclass

@dataclass
class SubscriptionItem:
    instrument: str
    jobid: int
