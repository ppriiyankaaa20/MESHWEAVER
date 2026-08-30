from dataclasses import dataclass
import hashlib
import time
from typing import List, Optional


def node_id_to_int(node_id: str) -> int:
    return int(hashlib.sha1(node_id.encode("utf-8")).hexdigest(), 16)


@dataclass
class Contact:
    node_id: str
    node_id_int: int
    host: str
    port: int
    last_seen: float = 0.0

    def __post_init__(self):
        if not self.last_seen:
            self.last_seen = time.time()


class KBucket:
    def __init__(self, k: int = 8):
        self.k = k
        self.contacts: List[Contact] = []

    def add(self, contact: Contact) -> bool:
        for idx, c in enumerate(self.contacts):
            if c.node_id == contact.node_id:
                self.contacts.pop(idx)
                contact.last_seen = time.time()
                self.contacts.append(contact)
                return True

        if len(self.contacts) < self.k:
            contact.last_seen = time.time()
            self.contacts.append(contact)
            return True

        return False

    def remove(self, node_id: str) -> bool:
        for idx, c in enumerate(self.contacts):
            if c.node_id == node_id:
                self.contacts.pop(idx)
                return True
        return False

    def get_contacts(self) -> List[Contact]:
        return list(self.contacts)


class RoutingTable:
    def __init__(self, node_id: str, k: int = 8):
        self.node_id = node_id
        self.node_id_int = node_id_to_int(node_id)
        self.k = k
        self.buckets: List[KBucket] = [KBucket(k=self.k) for _ in range(160)]

    def _get_bucket_index(self, target_int: int) -> int:
        xor_val = self.node_id_int ^ target_int
        if xor_val == 0:
            return 0
        return min(159, xor_val.bit_length() - 1)

    def add_contact(self, contact: Contact) -> bool:
        if contact.node_id == self.node_id:
            return False
        idx = self._get_bucket_index(contact.node_id_int)
        return self.buckets[idx].add(contact)

    def remove_contact(self, node_id: str) -> None:
        target_int = node_id_to_int(node_id)
        idx = self._get_bucket_index(target_int)
        self.buckets[idx].remove(node_id)

    def find_closest_nodes(self, target_id_int: int, count: Optional[int] = None) -> List[Contact]:
        count = count or self.k
        all_contacts: List[Contact] = []
        for bucket in self.buckets:
            all_contacts.extend(bucket.get_contacts())

        all_contacts.sort(key=lambda c: c.node_id_int ^ target_id_int)
        return all_contacts[:count]

    def get_all_contacts(self) -> List[Contact]:
        all_contacts: List[Contact] = []
        for bucket in self.buckets:
            all_contacts.extend(bucket.get_contacts())
        return all_contacts
