import avro.schema
import avro.io
import io
import random
import string
import time
import tracemalloc
import json
import uuid
from datetime import datetime

# Avro Event Format for CloudEvents
# https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/formats/avro-format.md
with open("benchmark/avro/cloudevents.avsc", "r") as f:
    schema = avro.schema.parse(f.read())

def generate_generic_event(id):
  return {
      "attribute": 
      {
        "specversion": "1.0",
        "id": int(id) * 5,
        "source": "urn:exads:ads/bidder",
        "type": "click",
        "datacontenttype": "application/json",
        "time": datetime.now().strftime("%H:%M:%S"),
        "payload": b"\x01\x10"
      },
      "data": {
        "id": int(id),
        "country": "US",
        "device": 1,
        "zone": 42,
        "pricing_model": 3,
        "bid_floor": 0.5,
        "smart_price": 0.75,
        "winning_price": 0.8,
        "event_type": 0,
        "network": 5
      } 
  }

def generate_larger_generic_event(id):
  return {
      "attribute": 
      {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        "source": "urn:exads:ads/bidder",
        "type": "click",
        "datacontenttype": "application/json",
        "time": datetime.now().strftime("%H:%M:%S"),
        "payload": b"\x01\x10",
        "processed": True,
        "priority": 5,
        "region": "us-west",
        "retry": False,
        "payload_type": "json",
        "checksum": b"\x9f\x8c\x7a",
        "error_code": None
      },
      "data": {
        "id": int(id),
        "country": "US",
        "device": 1,
        "zone": 42,
        "data_string": {
          "value": {
            "random_string": ''.join(random.choices(string.ascii_letters + string.digits, k=300)) # create a 300-character long random string 
          }
        },
        "data_json": {
          "value": {
                "is_active": True,
                "user_id": 12345,
                "price": 99.99,
                "username": "user_example",
                "is_verified": False,
                "age": 30,
                "discount_rate": 0.15,
                "email": "user@example.com",
                "has_premium": True,
                "rating": 4.7,
                "login_attempts": 5,
                "account_balance": 1500.75,
                "country": "US",
                "subscription_level": "gold",
                "notifications_enabled": False,
                "last_login_days_ago": 2,
                "max_storage_gb": 100,
                "cpu_cores": 8,
                "temperature_celsius": 23.5,
                "favorite_color": "blue",
                "email_verified": True,
                "items_in_cart": 3,
                "session_time_minutes": 45.2,
                "bio": "Loves coding and coffee",
                "is_admin": False,
                "posts_count": 128,
                "average_response_time": 0.342,
                "timezone": "UTC-5",
                "notifications_count": 12,
                "is_beta_user": True
            }
        },
        "pricing_model": 3,
        "bid_floor": 0.5,
        "smart_price": 0.75,
        "winning_price": 0.8,
        "event_type": 0,
        "network": 5,
        "cloud_event_data": {
           "value": {
               "data": [
                  {
                    "value": {
                      "key1": None,
                      "key2": True,
                      "key3": 42.0,
                      "key4": "example string",
                      "key5": {
                        "nestedKey": {
                          "value": {
                            "innerKey": "innerValue"
                          }
                        }
                      }
                    }
                  },
                  {
                    "value": {
                      "anotherKey": "anotherValue",
                      "number": 3.14
                    }
                  },
                ]
            }
        }
      } 
  }

def serialize(user):
  buffer = io.BytesIO()
  encoder = avro.io.BinaryEncoder(buffer)
  writer = avro.io.DatumWriter(schema)
  writer.write(user, encoder)
  return buffer.getvalue()

def run(num_messages=10000):
  print("\nStarting simple event with messages ...\n")
  total_size = 0
  start_time = time.time()
  tracemalloc.start()

  for i in range(num_messages):
    event = generate_generic_event(i)
    avro_bytes = serialize(event)
    total_size += len(avro_bytes)

  current, peak = tracemalloc.get_traced_memory()
  tracemalloc.stop()
  elapsed = time.time() - start_time

  print(f"Messages processed: {num_messages}")
  print(f"Total size: {total_size / 1024:.2f} KB")
  print(f"Avg size per message: {total_size / num_messages:.2f} bytes")
  print(f"Time taken: {elapsed:.4f} seconds")
  print(f"Avg time per message: {elapsed / num_messages * 1000:.4f} ms")
  print(f"Peak memory usage: {peak / 1024:.2f} KB")
  print("\nEnding simple event ...\n")
  
  print("\n...............................................................\n")

  print("\nStarting larger event with messages...\n")
  total_size = 0
  start_time = time.time()
  tracemalloc.start()

  for i in range(num_messages):
    event = generate_larger_generic_event(i)
    avro_bytes = serialize(event)
    total_size += len(avro_bytes)

  current, peak_large = tracemalloc.get_traced_memory()
  tracemalloc.stop()
  elapsed_large = time.time() - start_time

  print(f"Messages processed: {num_messages}")
  print(f"Total size: {total_size / 1024:.2f} KB")
  print(f"Avg size per message: {total_size / num_messages:.2f} bytes")
  print(f"Time taken: {elapsed_large:.4f} seconds")
  print(f"Avg time per message: {elapsed_large / num_messages * 1000:.4f} ms")
  print(f"Peak memory usage: {peak_large / 1024:.2f} KB")

  print("\nEnding larger event ...\n")

if __name__ == "__main__":
  run(1000)

