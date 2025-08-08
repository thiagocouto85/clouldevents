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
        "specversion": ''.join(random.choices(string.ascii_letters + string.digits, k=5)),
        "id": str(uuid.uuid4()),
        "source":''.join(random.choices(string.ascii_letters + string.digits, k=300)),
        "type": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
        "datacontenttype": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
        "time": datetime.now().strftime("%H:%M:%S"),
        "payload": b"\x01\x10"
      },
      "data": {
        "id": int(id),
        "country": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
        "device": random.randint(0, 100),
        "zone": random.randint(0, 100),
        "pricing_model": random.randint(0, 100),
        "bid_floor": random.uniform(0, 100),
        "smart_price": random.uniform(0, 100),
        "winning_price": random.uniform(0, 100),
        "event_type": random.randint(0, 100),
        "network": random.randint(0, 100)
      } 
  }

def generate_larger_generic_event(id):
  return {
      "attribute": 
      {
        "specversion":  ''.join(random.choices(string.ascii_letters + string.digits, k=5)),
        "id": str(uuid.uuid4()),
        "source": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
        "type": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
        "datacontenttype": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
        "time": datetime.now().strftime("%H:%M:%S"),
        "payload": random.randbytes(8),
        "processed": random.randint(0, 1),
        "priority": random.randint(0, 100),
        "region": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
        "retry": random.randint(0, 1),
        "payload_type": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
        "checksum": random.randbytes(8),
        "error_code": None
      },
      "data": {
        "id": int(id),
        "country": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
        "device": random.randint(0, 100),
        "zone": random.randint(0, 100),
        "data_string": {
          "value": {
            "random_string": ''.join(random.choices(string.ascii_letters + string.digits, k=300)) # create a 300-character long random string 
          }
        },
        "data_json": {
          "value": {
                "is_active": random.randint(0, 1),
                "user_id": str(uuid.uuid4()),
                "price": random.uniform(0, 100),
                "username": "user_example",
                "is_verified": random.randint(0, 1),
                "age": random.randint(0, 100),
                "discount_rate": random.uniform(0, 100),
                "email": "user@example.com",
                "has_premium": random.randint(0, 1),
                "rating": random.uniform(0, 100),
                "login_attempts": random.randint(0, 100),
                "account_balance": random.uniform(0, 100),
                "country": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
                "subscription_level": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
                "notifications_enabled": random.randint(0, 1),
                "last_login_days_ago": random.randint(0, 100),
                "max_storage_gb": random.randint(0, 100),
                "cpu_cores": random.randint(0, 100),
                "temperature_celsius": random.uniform(0, 100),
                "favorite_color": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
                "email_verified": random.randint(0, 1),
                "items_in_cart": random.randint(0, 1),
                "session_time_minutes": random.uniform(0, 100),
                "bio": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
                "is_admin": random.randint(0, 1),
                "posts_count": random.randint(0, 100),
                "average_response_time": random.uniform(0, 100),
                "timezone": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
                "notifications_count": random.randint(0, 100),
                "is_beta_user": random.randint(0, 1)
            }
        },
        "pricing_model": random.randint(0, 100),
        "bid_floor": random.uniform(0, 100),
        "smart_price": random.uniform(0, 100),
        "winning_price": random.uniform(0, 100),
        "event_type": random.randint(0, 100),
        "network": random.randint(0, 100),
        "cloud_event_data": {
           "value": {
               "data": [
                  {
                    "value": {
                      "key1": None,
                      "key2": random.randint(0, 1),
                      "key3": random.uniform(0, 100),
                      "key4": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
                      "key5": {
                        "nestedKey": {
                          "value": {
                            "innerKey": ''.join(random.choices(string.ascii_letters + string.digits, k=300))
                          }
                        }
                      }
                    }
                  },
                  {
                    "value": {
                      "anotherKey": ''.join(random.choices(string.ascii_letters + string.digits, k=300)),
                      "number": random.uniform(0, 100)
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

