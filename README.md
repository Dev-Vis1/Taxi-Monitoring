# 🚖 Real-Time Taxi Fleet Monitoring System
-----

## 🧭 Overview

This system provides high-throughput, real-time monitoring for Beijing's taxi fleet. It processes GPS streams to calculate instantaneous and average speeds, track distance traveled, detect geofence violations, and trigger operational alerts based on configurable thresholds.

-----

## 🌐 Live Deployment
You can access our real-time dashboard hosted on a public cloud instance: http://35.209.186.27:8050

-----

## 💡 Project Insights
We’ve built a dedicated website that explains our project architecture, data flow, implementation details, and performance optimizations in a visual and interactive manner:

[👉 Explore the Project Explanation Website](https://big-data-explanation.vercel.app/)

-----

## ⚙️ Key Features

| Feature              | Implementation Details                       |
| :------------------- | :------------------------------------------- |
| **Real-time Processing** | Kafka + Flink pipeline with 50ms latency     |
| **Geofence Monitoring** | 10km radius from Forbidden City              |
| **Speed Alerts** | $\>50$ km/h threshold detection                 |
| **Dashboard** | Live updates with 10,000+ taxi visualization |
| **Fault Tolerance** | Flink checkpoints + Kafka replication        |

### 🧰 Tech Stack

The system integrates multiple technologies for efficient real-time processing and visualization:

  * **Apache Flink (Java)** – Distributed stream processing
  * **Apache Kafka** – High-throughput event streaming
  * **Redis** – Fast in-memory state storage and metrics
  * **Docker** – Containerized deployment and orchestration
  * **Dash (Python)** – Interactive real-time web dashboard

-----

## 🚀 Operations Guide

### 🔧 Quick Start

To get the system up and running quickly, follow these steps:

**1. Pull required images:**

```bash
docker pull parth9969/bd25_project_a6_b:producer
```
```bash
docker pull parth9969/bd25_project_a6_b:dash
```

**2. Initialize system:**

```bash
docker-compose up -d --build
```

**3. Build processing job :**

  * **Windows:**

    ```bash
    docker run -it --rm ^
      -v "%cd%\taxi_locations\consumer\taxi_flink:/app" ^
      -w /app ^
      maven:3.8.6-eclipse-temurin-17 ^
      mvn clean package
    ```

  * **macOS/Linux:**

    ```bash
    docker run -it --rm \
      -v "$(pwd)/taxi_locations/consumer/taxi_flink:/app" \
      -w /app \
      maven:3.8.6-eclipse-temurin-17 \
      mvn clean package
    ```

**4. Deploy Flink job (copy JAR): (Run from one level under root i.e., inside `taxi_locations/`)**

```bash
docker cp consumer/taxi_flink/target/taxi_locations-1.jar flink-jobmanager:/opt/flink
```

**5. Start Flink job:**

```bash
docker exec flink-jobmanager flink run -d /opt/flink/taxi_locations-1.jar
```

**6. Access dashboard:**

Navigate to `http://localhost:8050` in your web browser.

-----

## 🔄 Kafka Management

**List Kafka topics:**

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic taxi-locations \
  --from-beginning \
  --max-messages 5
```

-----

## 📦 Redis Inspection


**Get average speed metrics:**

```bash
docker exec redis redis-cli HGETALL metrics:avgSpeed
```

**Get Redis DB size:**

```bash
docker exec -it redis redis-cli DBSIZE
```

-----

## ⚙️ Flink Operations


**List running Flink jobs:**

```bash
docker exec flink-jobmanager flink list
```

**View Flink job metrics (replace `<job-id>` with the actual job ID):**

```bash
docker exec flink-jobmanager flink metrics <job-id>
```

-----

## 📊 Dashboard Management

**Rebuild and restart dashboard:**

```bash
docker-compose stop dash-app && 
docker-compose rm -f dash-app &&
docker-compose build --no-cache dash-app && 
docker-compose up -d dash-app
```

-----

## ☁️ Cloud & DevOps Commands

### 🛠️ Install Essentials (Ubuntu)

**Install Git:**

```bash
sudo apt update && sudo apt install git -y
```

**Install Docker:**

```bash
curl -fsSL https://get.docker.com | sh
```

**Install Screen:**

```bash
sudo apt install screen -y
```

**Install Cloudflared:**

```bash
wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb && sudo dpkg -i cloudflared-linux-amd64.deb
```

### 🖥️ Screen Usage

**Start new session:**

```bash
screen -S taxiapp
```

**List sessions:**

```bash
screen -ls
```

### 🌐 Cloudflared Tunnel

**Expose dashboard:**

```bash
cloudflared tunnel --url http://localhost:8050
```

-----

## 🧹 Optional Cleanup Commands

**Remove Flink checkpoints:**

```bash
sudo rm -rf flink-checkpoints/*
```

**List Flink checkpoint files:**

```bash
ls -la flink-checkpoints
```

**Prune Docker resources:**

```bash
docker system prune -f
```

-----

## 🧪 Performance Characteristics

### 📊 Benchmark Results

| Metric               | Before Optimization | After Optimization |
| -------------------- | ------------------- | ------------------ |
| Processing Time      | 10+ hours           | 3–5 minutes        |
| Throughput           | \~10 records/sec    | 100k+ records/sec  |
| File Processing Rate | 1 file / 2 min      | 50–100 files/sec   |
| Max Taxis Processed  | 500                 | All 10,357         |


-----

## ✅ Validation

**Verification Checks:**

1. Throughput ≥ 100k records/sec

2. All 10,357 files processed

3. Processing time < 5 minutes

4. Dashboard updates within 1s of even

-----

## 💻 System Requirements

| Resource | Minimum           | Recommended (Production) |
| :------- | :---------------- | :----------------------- |
| **CPU** | 4 cores           | 16+ cores                |
| **RAM** | 8GB               | 32GB                     |
| **Storage** | SSD with 5GB free | NVMe SSD                 |
| **Network** | 1Gbps             | 10Gbps                   |

-----

## 👥 Contributions

This project was a collaborative effort. Here are the key contributions from each team member:

| Contributor         | Key Contributions                                                                                   |
| :------------------ | :-------------------------------------------------------------------------------------------------- |
| **Sunmeet Kohli** | Dashboard UI/UX design, Flink MainJob logic, map visualization, incident display, interaction logic |
| **Parthav Pillai** | Kafka producer setup, Redis client optimization, batch processing, performance tuning               |
| **Sanika Acharya** | Flink-Redis sink, UI enhancements, incident/event logic, Redis schema, documentation                |
| **Matthew Ayodele** | DTO design, cloud deployment scripts, Docker orchestration, system integration                      |