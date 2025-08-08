import json
from cloudevents_pb2 import CloudEvent
from google.protobuf import timestamp_pb2
from google.protobuf.any_pb2 import Any
from datetime import datetime
import time
import tracemalloc
import uuid
import random
import string

def create_cloud_event(id):
    event = CloudEvent()

    # Required Attributes
    event.id = str(uuid.uuid4())
    event.source = "/my/source"
    event.spec_version = "1.0"
    event.type = "click"


    # Optional and Extension Attributes
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(datetime.utcnow())
    attr_val_ts = CloudEvent.CloudEventAttributeValue()
    attr_val_ts.ce_timestamp.CopyFrom(ts)
    event.attributes["time"].CopyFrom(attr_val_ts)

    datacontent_type = CloudEvent.CloudEventAttributeValue()
    datacontent_type.ce_uri = "application/json"
    event.attributes["datacontenttype"].CopyFrom(datacontent_type)

    payload = CloudEvent.CloudEventAttributeValue()
    payload.ce_bytes = b"\x01\x10"
    event.attributes["payload"].CopyFrom(payload)

    # Set data
    data = {
        "id": int(id),
        "country": '' . join(random.choices(string.ascii_letters + string.digits, k=300)),
        "device": random.randint(0, 100),
        "zone": random.randint(0, 100),
        "pricing_model": random.randint(0, 100),
        "bid_floor": random.uniform(0, 100),
        "smart_price": random.uniform(0, 100),
        "winning_price": random.uniform(0, 100),
        "event_type": random.randint(0, 100),
        "network": random.randint(0, 100)
    }
    event.text_data = json.dumps(data)
    
    return event

def create_cloud_event_large(id):
    event = CloudEvent()

    # Required Attributes
    event.id = str(uuid.uuid4())
    event.source = "/my/source"
    event.spec_version = "1.0"
    event.type = "click"

    # Optional and Extension Attributes
    processed = CloudEvent.CloudEventAttributeValue()
    processed.ce_boolean = True
    event.attributes["processed"].CopyFrom(processed)

    attr_val_int = CloudEvent.CloudEventAttributeValue()
    attr_val_int.ce_integer = 42
    event.attributes["count"].CopyFrom(attr_val_int)

    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(datetime.utcnow())
    attr_val_ts = CloudEvent.CloudEventAttributeValue()
    attr_val_ts.ce_timestamp.CopyFrom(ts)
    event.attributes["time"].CopyFrom(attr_val_ts)

    datacontent_type = CloudEvent.CloudEventAttributeValue()
    datacontent_type.ce_uri = "application/json"
    event.attributes["datacontenttype"].CopyFrom(datacontent_type)

    payload = CloudEvent.CloudEventAttributeValue()
    payload.ce_bytes = b"\x01\x10"
    event.attributes["datacontenttype"].CopyFrom(payload)

    priority = CloudEvent.CloudEventAttributeValue()
    priority.ce_integer = 5
    event.attributes["priority"].CopyFrom(priority)

    region = CloudEvent.CloudEventAttributeValue()
    region.ce_uri = "us-west"
    event.attributes["region"].CopyFrom(region)

    retry = CloudEvent.CloudEventAttributeValue()
    retry.ce_boolean = False
    event.attributes["retry"].CopyFrom(retry)

    payload_type = CloudEvent.CloudEventAttributeValue()
    payload_type.ce_uri = "json"
    event.attributes["payload_type"].CopyFrom(payload_type)

    checksum = CloudEvent.CloudEventAttributeValue()
    checksum.ce_bytes = b"\x9f\x8c\x7a"
    event.attributes["datacontenttype"].CopyFrom(checksum)

    empty_attr = CloudEvent.CloudEventAttributeValue()  # no attr set = None
    event.attributes["error_code"].CopyFrom(empty_attr)

    # Set data
    data = {
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
    event.text_data = json.dumps(data)
    
    return event


def main(num_messages=10000):
    print("\nStarting simple event ...\n")
    total_size = 0
    start_time = time.time()
    tracemalloc.start()

    for i in range(num_messages):
        cloud_event = create_cloud_event(i)
        # SerializeToString(): serializes the message and returns it as a string. 
        # Note that the bytes are binary, not text; we only use the str type as a convenient container.
        # https://protobuf.dev/getting-started/pythontutorial/
        avro_bytes = cloud_event.SerializeToString()
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

    print("\nStarting large event ...\n")
    total_size = 0
    start_time = time.time()
    tracemalloc.start()

    for i in range(num_messages):
        cloud_event = create_cloud_event_large(i)
        # SerializeToString(): serializes the message and returns it as a string. 
        # Note that the bytes are binary, not text; we only use the str type as a convenient container.
        # https://protobuf.dev/getting-started/pythontutorial/
        avro_bytes = cloud_event.SerializeToString()
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
    print("\nEnding large event ...\n")

  
# https://protobuf.dev/
if __name__ == "__main__":
    main(1000)