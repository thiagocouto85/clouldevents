docker-compose up --build      

docker-compose run python_app python benchmark/avro/avro_benchmark.py

docker-compose run python_app python benchmark/protobuf/protobuf_benchmark.py




add pb2 manually  protoc --proto_path=benchmark/protobuf --python_out=benchmark/protobuf benchmark/protobuf/cloudevents.proto



Avro
# active  -> source venv/bin/activate


