import influxdb_client, os, time, sys, socket, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("INFLUXDB_TOKEN")
org = "EPSEVG" #your organization
url = "http://localhost:8086"
bucket = "SMAC-EK" #your bucket

def main():
    try:
        write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
    except Exception as e:
        print('Error initializing Influx client.', e)

    try:
        s = socket.socket()
        s.bind(('0.0.0.0', 8090))
        s.listen(0)
    except Exception as e:
        print('Error initializing socket binding.', e)

    print('Influx client and Socket initialized successfully, ready to start transmission.')
    time.sleep(2)
    start_time = time.time()
    v_content = []

    while True:
        client_socket, addr = s.accept()
        buffer = b''  # Buffer to accumulate data

        while True:
            content = client_socket.recv(16)
            if len(content) == 0:
                break

            buffer += content

            # Process complete messages
            while b'\n' in buffer:
                message, buffer = buffer.split(b'\n', 1)
                message_str = message.decode('utf-8')

                # Store the data with timestamp
                data = {'content': message_str, 'time': time.time()}
                v_content.append(data)

            if time.time() - start_time > 5:
                break

        client_socket.close()
        break

    # Send data to InfluxDB
    write_api = write_client.write_api(write_options=SYNCHRONOUS)
    for data in v_content:
        tag_content = data['content']
        timestamp = int(data['time'] * 1e9)  # Convert to nanoseconds for InfluxDB

        # Parse message to extract key-value
        try:
            key, value = tag_content.split(": ")
            # if key == 'IR':
            #     value = int(value, 0)  # Convert the HEX value to integer
            if key == 'light':
                value = int(value)

        except ValueError:
            print(f"Failed to parse message: {tag_content}")
            continue  # Skip to the next message if parsing fails

        # Create the data point
        if key == 'IR':
            measure_point = Point('IR')
        elif key == 'light':
            measure_point = Point('light')
        point = (
            measure_point
            .tag("sensor", key)
            .field("value", value)
            .time(timestamp)
        )

        # Write the point to InfluxDB
        write_api.write(bucket=bucket, org=org, record=point)

    print("Closing connection")
    client_socket.close()

if __name__ == '__main__':
    sys.exit(main()) 
