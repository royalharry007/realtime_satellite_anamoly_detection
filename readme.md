# 🛰️ GNSS Real-Time Anomaly Detection System

A real-time system for processing GNSS NMEA data streams and detecting anomalies using Apache Kafka and Apache Spark Streaming.

## Overview

This system processes NMEA data from GNSS sources and detects anomalies in real-time:

- **Signal Drops**: SNR below threshold
- **Position Drift**: Unexpected position changes  
- **Satellite Loss**: Insufficient satellites in view

## Features

- Real-time NMEA parsing (GGA, GSV, GSA, RMC messages)
- Kafka-based message streaming
- Spark Streaming for distributed processing
- Configurable anomaly detection thresholds
- CSV/Parquet output storage
- Streamlit dashboard for visualization

## System Architecture

```
NMEA Data → Kafka Producer → Kafka → Spark Consumer → Storage/Dashboard
```

## Prerequisites

- macOS 12.0+ or compatible OS
- Python 3.9+
- Java 11
- Homebrew (for macOS)

## Installation

### 1. Install System Dependencies

```bash
# Install Java 11
brew install openjdk@11

# Install Kafka
brew install kafka

# Install Spark
brew install apache-spark

# Add to PATH
echo 'export JAVA_HOME="/opt/homebrew/opt/openjdk@11"' >> ~/.zshrc
echo 'export SPARK_HOME="/opt/homebrew/opt/apache-spark/libexec"' >> ~/.zshrc
source ~/.zshrc
```

### 2. Install Python Dependencies

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install requirements
pip install -r requirements.txt
```

### 3. Configure the System

Edit `config/settings.yaml` with your settings:

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "gnss_topic"

spark:
  app_name: "GNSS_Anomaly_Detection"
  master: "local[*]"
  batch_interval: 2

anomaly_detection:
  snr_threshold: 20.0
  drift_threshold: 0.0001
  min_satellites: 5

storage:
  output_dir: "./results"
  csv_enabled: true
  parquet_enabled: true
```

## Usage

### Quick Start

Use the startup script to run all components:

```bash
./run_system.sh
```

### Manual Startup

Open 4 separate terminals:

**Terminal 1 - Zookeeper:**
```bash
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties
```

**Terminal 2 - Kafka:**
```bash
kafka-server-start /opt/homebrew/etc/kafka/server.properties

# Create topic (after Kafka starts)
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic gnss_topic
```

**Terminal 3 - Spark Consumer:**
```bash
source venv/bin/activate
python consumer/spark_consumer.py
```

**Terminal 4 - Kafka Producer:**
```bash
source venv/bin/activate

# Test mode
python producer/kafka_producer.py --source test --delay 2

# File mode
python producer/kafka_producer.py --source file --file data/sample_nmea.txt
```

**Terminal 5 - Dashboard (Optional):**
```bash
source venv/bin/activate
streamlit run dashboard/app.py
```

## Project Structure

```
gnss-project/
├── config/
│   └── settings.yaml          # System configuration
├── consumer/
│   └── spark_consumer.py      # Spark streaming consumer
├── dashboard/
│   └── app.py                 # Streamlit dashboard
├── data/
│   ├── sample_nmea.txt        # Sample NMEA data
│   └── generate_sample.py     # Test data generator
├── producer/
│   └── kafka_producer.py      # Kafka producer
├── utils/
│   ├── nmea_parser.py         # NMEA message parser
│   └── ntrip_client.py        # NTRIP client (optional)
├── results/                   # Output directory
├── checkpoints/               # Spark checkpoints
├── requirements.txt
└── run_system.sh             # Startup script
```

## Testing

### Test Kafka Connection

```bash
# Consumer test
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic gnss_topic \
  --from-beginning
```

### Test with Sample Data

```bash
# Generate test data
python data/generate_sample.py

# Run producer with test data
python producer/kafka_producer.py --source file --file data/sample_nmea.txt
```

## Troubleshooting

### Kafka Connection Issues

```bash
# Check if Kafka is running
ps aux | grep kafka

# Restart Kafka
kafka-server-stop
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

### Port Conflicts

```bash
# Find process on port
lsof -i :9092  # Kafka
lsof -i :8501  # Streamlit

# Kill process
kill -9 <PID>
```

### Java Version Issues

```bash
# Verify Java 11
java -version

# Set JAVA_HOME if needed
export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
```

## Configuration

Key configuration parameters in `config/settings.yaml`:

- `snr_threshold`: Minimum acceptable SNR (default: 20.0 dB)
- `drift_threshold`: Maximum position change (default: 0.0001°)
- `min_satellites`: Minimum satellites required (default: 5)
- `batch_interval`: Spark processing interval (default: 2 seconds)

## Output

Results are saved to the `results/` directory:

- `results/anomalies_csv/` - CSV format
- `results/anomalies_parquet/` - Parquet format

## Dependencies

Core dependencies:
- kafka-python 2.0.2
- pyspark 3.5.0
- pandas 2.1.3
- pynmea2 1.19.0
- streamlit 1.28.2
- pyyaml 6.0.1

See `requirements.txt` for complete list.

## License

MIT License

## Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Streaming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [NMEA 0183 Protocol](https://www.nmea.org/content/STANDARDS/NMEA_0183_Standard)

---

Built for real-time GNSS data processing and anomaly detection