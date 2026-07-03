#!/usr/bin/env python3
"""
Hotel Bulk Loader - Standalone script for parallel bulk document insertion
Generates random hotel documents and uses Couchbase Python SDK for bulk operations
"""

import json
import random
import sys
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions, InsertOptions
from couchbase.exceptions import DocumentExistsException, DocumentNotFoundException


class HotelDataGenerator:
    """Generates random hotel documents matching the hotel dataset schema"""
    
    # Names that will match query patterns:
    # - "name like '%%Dil%%'" matches: Dil, Dilbert, Dillan, Dillon, Adil, Dila, Dillard, Dilworth
    # - "r.author LIKE 'M%%'" matches names starting with M: Maryanna, Marcel, Minda, Michael, etc.
    FIRST_NAMES = [
        "Willard", "Jorge", "Barbera", "Della", "Belkis", "Zola", "Tiffanie", "Kyle",
        "Gary", "Maryanna", "Marcel", "Minda", "Ronny", "Thompson", "Shields", "Ferry",
        "Waelchi", "Boyle", "Windler", "Rosenbaum", "O'Keefe", "Farrell", "Pouros",
        "Hermiston", "Klocko", "Hilpert", "James", "Sharon", "Robert", "Linda", "Michael",
        "Dil", "Dilbert", "Dillan", "Dillon", "Adil", "Dila",  # Names with 'Dil' pattern
        "Mary", "Marcus", "Maria", "Martin", "Maxwell", "Maya", "Mitchell", "Morgan",  # Names starting with M
        "Matthew", "Madison", "Melissa", "Monica", "Michelle", "Marvin"  # More M names
    ]
    
    LAST_NAMES = [
        "Shields", "Ferry", "Waelchi", "Boyle", "Windler", "Rosenbaum", "O'Keefe",
        "Farrell", "Pouros", "Hermiston", "Klocko", "Hilpert", "Thompson", "Johnson",
        "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
        "Anderson", "Taylor", "Thomas", "Moore", "Jackson", "Martin", "Lee", "Walker",
        "Dillon", "Dillard", "Dilworth"  # Last names with 'Dil' pattern
    ]
    
    HOTEL_TYPES = ["Hotel", "Inn", "Resort", "Lodge", "Suites", "Motel", "Hostel", "B&B"]
    
    # Countries that match query patterns:
    # - "%%F%%" matches: France, South Africa, Afghanistan
    # - "%%ra%%" matches: France, Australia, Central African Republic, Israel, Iraq, Iran
    # - "Greece" exact match
    COUNTRIES = [
        "France",  # Matches both %%F%% and %%ra%%
        "Australia",  # Matches %%ra%%
        "Greece",  # Exact match needed
        "United States",
        "United Kingdom",
        "South Africa",  # Matches %%F%%
        "Afghanistan",  # Matches %%F%%
        "Central African Republic",  # Matches both %%F%% and %%ra%%
        "Israel",  # Matches %%ra%%
        "Iraq",  # Matches %%ra%%
        "Iran",  # Matches %%ra%%
        "Japan",
        "Germany",
        "Canada",
        "Spain",
        "Netherlands",
        "Italy",
        "Thailand",
        "China",
        "India",
        "Brazil",
        "Mexico",
        "South Korea",
        "Switzerland",
        "Norway",
        "Ukraine",  # Matches %%ra%%
        "Emirates"  # Matches %%ra%%
    ]
    
    @staticmethod
    def generate_hotel_document(hotel_id: str) -> Dict[str, Any]:
        """Generate a single random hotel document matching the exact schema"""
        # Generate random name (just first and last name)
        name = f"{random.choice(HotelDataGenerator.FIRST_NAMES)} {random.choice(HotelDataGenerator.LAST_NAMES)}"
        country = random.choice(HotelDataGenerator.COUNTRIES)
        hotel_type = random.choice(HotelDataGenerator.HOTEL_TYPES)
        
        # Generate reviews (2-10 reviews per hotel)
        num_reviews = random.randint(2, 10)
        reviews = []
        base_date = datetime.now()
        for i in range(num_reviews):
            review_date = base_date + timedelta(weeks=i)
            reviews.append({
                "date": review_date.strftime("%Y-%m-%d %H:%M:%S"),
                "author": f"{random.choice(HotelDataGenerator.FIRST_NAMES)} {random.choice(HotelDataGenerator.LAST_NAMES)}",
                "ratings": {
                    "Value": random.randint(0, 4),
                    "Cleanliness": random.randint(0, 4),
                    "Overall": random.randint(0, 4),
                    "Check in / front desk": random.randint(0, 4),
                    "Rooms": random.randint(0, 4)
                }
            })
        
        # Generate public likes (0-10 full names)
        num_likes = random.randint(0, 10)
        public_likes = [f"{random.choice(HotelDataGenerator.FIRST_NAMES)} {random.choice(HotelDataGenerator.LAST_NAMES)}" 
                       for _ in range(num_likes)]
        
        # Generate random address with specific format
        street_num = random.randint(1, 9999)
        street_name = random.choice(['Shawanna', 'Maple', 'Oak', 'Cedar', 'Pine', 'Elm', 'River', 'Lake'])
        street_type = random.choice(['Cape', 'Street', 'Avenue', 'Boulevard', 'Road', 'Way', 'Drive', 'Lane'])
        address = f"{street_num} {street_name} {street_type}"
        
        # Generate city name with 'side' suffix (like Erwinside)
        city_prefix = random.choice(['Erwin', 'Jack', 'River', 'Lake', 'Hill', 'Green', 'Wood', 'Spring'])
        city = f"{city_prefix}side"
        
        # Generate URL (format: www.firstname-lastname.biz/com/net)
        url_name = name.lower().replace(' ', '-')
        url_ext = random.choice(['biz', 'com', 'net', 'org'])
        url = f"www.{url_name}.{url_ext}"
        
        # Generate phone with specific format
        phone = f"({random.randint(100, 999)}) {random.randint(100, 999)}-{random.randint(1000, 9999)}"
        
        # Generate email (format: Firstname.Lastname@hotels.com)
        email_first = random.choice(HotelDataGenerator.FIRST_NAMES)
        email_last = random.choice(HotelDataGenerator.LAST_NAMES)
        email = f"{email_first}.{email_last}@hotels.com"
        
        # Generate 384-dimensional vector for desc_vectors field (random floats between -1.0 and 1.0)
        desc_vectors = [random.uniform(-1.0, 1.0) for _ in range(384)]
        
        # Build the hotel document matching exact schema
        hotel_doc = {
            "country": country,
            "mutate": 0,
            "address": address,
            "free_parking": random.choice([True, False]),
            "city": city,
            "type": hotel_type,
            "url": url,
            "reviews": reviews,
            "phone": phone,
            "price": random.randint(500, 2500),
            "avg_rating": round(random.uniform(0.0, 5.0), 16),
            "free_breakfast": random.choice([True, False]),
            "name": name,
            "public_likes": public_likes,
            "email": email,
            "desc_vectors": desc_vectors
        }
        
        return hotel_doc
    
    @staticmethod
    def generate_mutation_fields() -> Dict[str, Any]:
        """Generate mutated fields for an existing document (for update mode)"""
        return {
            "price": random.randint(500, 2500),
            "avg_rating": round(random.uniform(0.0, 5.0), 16),
            "desc_vectors": [random.uniform(-1.0, 1.0) for _ in range(384)]
        }


class HotelBulkLoader:
    """Handles bulk loading of hotel documents to Couchbase"""
    
    def __init__(self, host: str, username: str, password: str, bucket: str, 
                 scope: str = "_default", collection: str = "_default"):
        """
        Initialize the bulk loader
        
        Args:
            host: Couchbase cluster host
            username: Cluster username
            password: Cluster password
            bucket: Bucket name
            scope: Scope name (default: _default)
            collection: Collection name (default: _default)
        """
        self.host = host
        self.username = username
        self.password = password
        self.bucket_name = bucket
        self.scope_name = scope
        self.collection_name = collection
        self.cluster = None
        self.collection = None
        
    def connect(self):
        """Establish connection to Couchbase cluster"""
        try:
            auth = PasswordAuthenticator(self.username, self.password)
            options = ClusterOptions(auth)
            self.cluster = Cluster(f"couchbase://{self.host}", options)
            
            # Wait for cluster to be ready
            self.cluster.wait_until_ready(timedelta(seconds=10))
            
            bucket = self.cluster.bucket(self.bucket_name)
            self.collection = bucket.scope(self.scope_name).collection(self.collection_name)
            
            print(f"✓ Connected to {self.host}/{self.bucket_name}.{self.scope_name}.{self.collection_name}")
            return True
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            return False
    
    def bulk_insert(self, num_docs: int, key_prefix: str = "hotel_", 
                   batch_size: int = 1000, start_id: int = 0, max_retries: int = 3) -> Dict[str, int]:
        """
        Bulk insert hotel documents with retry logic
        
        Args:
            num_docs: Number of documents to insert
            key_prefix: Prefix for document keys
            batch_size: Number of documents to insert per batch
            start_id: Starting ID for document generation
            max_retries: Maximum retry attempts for failed documents
            
        Returns:
            Dictionary with success/failure counts
        """
        stats = {
            "success": 0,
            "failed": 0,
            "total": num_docs,
            "time_taken": 0,
            "retries": 0
        }
        
        start_time = time.time()
        failed_docs = {}  # Track failed documents for retry
        
        for batch_start in range(0, num_docs, batch_size):
            batch_end = min(batch_start + batch_size, num_docs)
            batch_docs = {}
            
            # Generate batch of documents
            for i in range(batch_start, batch_end):
                doc_id = start_id + i
                key = f"{key_prefix}{doc_id}"
                hotel_doc = HotelDataGenerator.generate_hotel_document(f"hotel_{doc_id}")
                batch_docs[key] = hotel_doc
            
            # Bulk insert using upsert_multi with retry logic
            retry_count = 0
            current_batch = batch_docs.copy()
            
            while retry_count <= max_retries and current_batch:
                try:
                    results = self.collection.upsert_multi(current_batch)
                    next_retry_batch = {}
                    
                    for key, result in results.results.items():
                        if result.success:
                            stats["success"] += 1
                        else:
                            # Queue for retry
                            if retry_count < max_retries:
                                next_retry_batch[key] = current_batch[key]
                                stats["retries"] += 1
                            else:
                                stats["failed"] += 1
                                print(f"  Failed to insert {key} after {max_retries} retries: {result.exception}")
                    
                    current_batch = next_retry_batch
                    retry_count += 1
                    
                    if current_batch and retry_count <= max_retries:
                        time.sleep(0.1 * retry_count)  # Exponential backoff
                        
                except Exception as e:
                    print(f"  Batch insert exception (attempt {retry_count + 1}): {e}")
                    retry_count += 1
                    if retry_count > max_retries:
                        stats["failed"] += len(current_batch)
                        current_batch = {}
                    else:
                        stats["retries"] += len(current_batch)
                        time.sleep(0.1 * retry_count)
            
            # Progress update
            if (batch_end % 5000 == 0) or (batch_end == num_docs):
                elapsed = time.time() - start_time
                rate = batch_end / elapsed if elapsed > 0 else 0
                print(f"  Progress: {batch_end}/{num_docs} docs ({rate:.0f} docs/sec)")
        
        stats["time_taken"] = time.time() - start_time
        return stats
    
    def bulk_update(self, num_docs: int, key_prefix: str = "hotel_",
                    batch_size: int = 1000, start_id: int = 0, 
                    ops_rate: int = 100, max_retries: int = 3) -> Dict[str, int]:
        """
        Bulk update existing hotel documents with mutated fields.
        Increments 'mutate' counter and regenerates price, avg_rating, and desc_vectors.
        
        Args:
            num_docs: Number of documents to update
            key_prefix: Prefix for document keys
            batch_size: Number of documents to update per batch
            start_id: Starting ID for document keys
            ops_rate: Target operations per second (for rate limiting)
            max_retries: Maximum retry attempts for failed documents
            
        Returns:
            Dictionary with success/failure counts
        """
        stats = {
            "success": 0,
            "failed": 0,
            "total": num_docs,
            "time_taken": 0,
            "retries": 0
        }
        
        start_time = time.time()
        ops_interval = 1.0 / ops_rate if ops_rate > 0 else 0
        last_op_time = start_time
        
        for batch_start in range(0, num_docs, batch_size):
            batch_end = min(batch_start + batch_size, num_docs)
            batch_docs = {}
            
            # Fetch existing documents and prepare mutations
            for i in range(batch_start, batch_end):
                doc_id = start_id + i
                key = f"{key_prefix}{doc_id}"
                
                try:
                    # Get existing document
                    result = self.collection.get(key)
                    doc = result.content_as[dict]
                    
                    # Apply mutations
                    mutation_fields = HotelDataGenerator.generate_mutation_fields()
                    doc["mutate"] = doc.get("mutate", 0) + 1
                    doc.update(mutation_fields)
                    
                    batch_docs[key] = doc
                except Exception as e:
                    # Document doesn't exist - skip it (don't create new docs during mutations)
                    # This ensures mutations only modify existing docs in place
                    pass
                
                # Rate limiting
                if ops_rate > 0:
                    current_time = time.time()
                    elapsed_since_last = current_time - last_op_time
                    if elapsed_since_last < ops_interval:
                        time.sleep(ops_interval - elapsed_since_last)
                    last_op_time = time.time()
            
            # Bulk upsert with retry logic
            retry_count = 0
            current_batch = batch_docs.copy()
            
            while retry_count <= max_retries and current_batch:
                try:
                    results = self.collection.upsert_multi(current_batch)
                    next_retry_batch = {}
                    
                    for key, result in results.results.items():
                        if result.success:
                            stats["success"] += 1
                        else:
                            if retry_count < max_retries:
                                next_retry_batch[key] = current_batch[key]
                                stats["retries"] += 1
                            else:
                                stats["failed"] += 1
                    
                    current_batch = next_retry_batch
                    retry_count += 1
                    
                    if current_batch and retry_count <= max_retries:
                        time.sleep(0.1 * retry_count)
                        
                except Exception as e:
                    print(f"  Batch update exception (attempt {retry_count + 1}): {e}")
                    retry_count += 1
                    if retry_count > max_retries:
                        stats["failed"] += len(current_batch)
                        current_batch = {}
                    else:
                        stats["retries"] += len(current_batch)
                        time.sleep(0.1 * retry_count)
            
            # Progress update
            if (batch_end % 5000 == 0) or (batch_end == num_docs):
                elapsed = time.time() - start_time
                rate = batch_end / elapsed if elapsed > 0 else 0
                print(f"  Update Progress: {batch_end}/{num_docs} docs ({rate:.0f} docs/sec)")
        
        stats["time_taken"] = time.time() - start_time
        return stats
    
    def bulk_delete(self, num_docs: int, key_prefix: str = "hotel_",
                    batch_size: int = 1000, start_id: int = 0) -> Dict[str, int]:
        """
        Bulk delete hotel documents by key range.

        Args:
            num_docs: Number of documents to delete
            key_prefix: Prefix for document keys
            batch_size: Number of documents to delete per batch
            start_id: Starting ID for document keys

        Returns:
            Dictionary with success/failure counts
        """
        stats = {
            "success": 0,
            "failed": 0,
            "total": num_docs,
            "time_taken": 0,
            "retries": 0
        }

        start_time = time.time()

        for batch_start in range(0, num_docs, batch_size):
            batch_end = min(batch_start + batch_size, num_docs)
            keys = [f"{key_prefix}{start_id + i}" for i in range(batch_start, batch_end)]

            try:
                results = self.collection.remove_multi(keys)
                for key, result in results.results.items():
                    if result.success:
                        stats["success"] += 1
                    else:
                        if not isinstance(result.exception, DocumentNotFoundException):
                            stats["failed"] += 1
                            print(f"  Failed to delete {key}: {result.exception}")
                        else:
                            stats["success"] += 1  # already gone — treat as success
            except Exception as e:
                print(f"  Batch delete exception: {e}")
                stats["failed"] += len(keys)

            if (batch_end % 5000 == 0) or (batch_end == num_docs):
                elapsed = time.time() - start_time
                rate = batch_end / elapsed if elapsed > 0 else 0
                print(f"  Delete Progress: {batch_end}/{num_docs} docs ({rate:.0f} docs/sec)")

        stats["time_taken"] = time.time() - start_time
        return stats

    def disconnect(self):
        """Close connection to Couchbase cluster"""
        if self.cluster:
            self.cluster.close()


def main():
    """Main entry point for standalone script execution"""
    if len(sys.argv) < 7:
        print("Usage: python hotel_bulk_loader.py <host> <username> <password> <bucket> <scope> <collection> <num_docs> [key_prefix] [batch_size] [start_id] [mode] [ops_rate]")
        print("\nModes:")
        print("  insert - Insert new documents (default)")
        print("  update - Update existing documents with mutations")
        print("  delete - Delete documents by key range")
        print("\nExample:")
        print("  python hotel_bulk_loader.py 127.0.0.1 Administrator password default scope_1 coll_1 10000 hotel_ 1000 0 insert")
        print("  python hotel_bulk_loader.py 127.0.0.1 Administrator password default scope_1 coll_1 10000 hotel_ 1000 0 update 100")
        print("  python hotel_bulk_loader.py 127.0.0.1 Administrator password default scope_1 coll_1 100 hotel_ 1000 1000 delete")
        sys.exit(1)
    
    host = sys.argv[1]
    username = sys.argv[2]
    password = sys.argv[3]
    bucket = sys.argv[4]
    scope = sys.argv[5]
    collection = sys.argv[6]
    num_docs = int(sys.argv[7])
    key_prefix = sys.argv[8] if len(sys.argv) > 8 else "hotel_"
    batch_size = int(sys.argv[9]) if len(sys.argv) > 9 else 1000
    start_id = int(sys.argv[10]) if len(sys.argv) > 10 else 0
    mode = sys.argv[11] if len(sys.argv) > 11 else "insert"
    ops_rate = int(sys.argv[12]) if len(sys.argv) > 12 else 100
    
    print(f"\n{'='*70}")
    print(f"Hotel Bulk Loader - {mode.upper()} Mode")
    print(f"{'='*70}")
    print(f"Target: {host}/{bucket}.{scope}.{collection}")
    print(f"Documents: {num_docs} (starting from ID {start_id})")
    print(f"Batch size: {batch_size}")
    if mode == "update":
        print(f"Ops rate: {ops_rate} ops/sec")
    print(f"{'='*70}\n")

    loader = HotelBulkLoader(host, username, password, bucket, scope, collection)

    if not loader.connect():
        sys.exit(1)

    try:
        if mode == "update":
            stats = loader.bulk_update(num_docs, key_prefix, batch_size, start_id, ops_rate)
            operation = "Update"
        elif mode == "delete":
            stats = loader.bulk_delete(num_docs, key_prefix, batch_size, start_id)
            operation = "Delete"
        else:
            stats = loader.bulk_insert(num_docs, key_prefix, batch_size, start_id)
            operation = "Insert"
        
        print(f"\n{'='*70}")
        print(f"Bulk {operation} Complete")
        print(f"{'='*70}")
        print(f"Success: {stats['success']}/{stats['total']}")
        print(f"Failed: {stats['failed']}/{stats['total']}")
        print(f"Retries: {stats['retries']}")
        print(f"Time: {stats['time_taken']:.2f} seconds")
        if stats['success'] > 0:
            print(f"Rate: {stats['success']/stats['time_taken']:.0f} docs/sec")
        print(f"{'='*70}\n")
        
        # Exit with error code if any documents failed
        if stats['failed'] > 0:
            sys.exit(1)
        
    finally:
        loader.disconnect()


if __name__ == "__main__":
    main()
