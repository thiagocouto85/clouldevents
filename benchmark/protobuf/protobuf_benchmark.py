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
    event.text_data = json.dumps(data)
    
    return event


def main(num_messages=10000):
    print("\nStarting simple event ...\n")
    total_size = 0
    start_time = time.time()
    tracemalloc.start()

    for i in range(num_messages):
        cloud_event = create_cloud_event(i)
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