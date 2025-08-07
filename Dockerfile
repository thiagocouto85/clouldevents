# Use an official Python base image
FROM python:3.10-slim

# Install pip dependencies
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy project files into the container
COPY . /app

# Install dependencies
#RUN pip install --no-cache-dir -r requirements.txt

# Install avro-python3 package
# RUN pip install --no-cache-dir avro-python3

# Generate cloudevents_pb2.py from .proto
RUN protoc --proto_path=benchmark/protobuf \
           --python_out=benchmark/protobuf \
           benchmark/protobuf/cloudevents.proto

# Install Python dependencies
RUN pip install --no-cache-dir avro-python3 protobuf

# Run the app
CMD ["python"]