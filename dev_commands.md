# 🛠 Development & Operations Command Guide

This guide includes essential commands for building the project, working with Kafka, deploying Flink jobs, and inspecting Redis data.

> 📍 **Note:** Run commands from the specified directory level as indicated in each section.

---

## ⚙️ 1. Build Project Using Dockerized Maven

Build the Java project using a temporary Maven container.

### ▶️ Windows (run from project root):
```bash
docker run -it --rm ^
  -v "%cd%\taxi_locations\consumer\taxi_flink:/app" ^
  -w /app ^
  maven:3.8.6-eclipse-temurin-17 ^
  mvn clean package
```

### ▶️ macOS/Linux (run from project root):
```bash
docker run -it --rm \
  -v "$(pwd)/taxi_locations/consumer/taxi_flink:/app" \
  -w /app \
  maven:3.8.6-eclipse-temurin-17 \
  mvn clean package
```

---

## 📡 2. Kafka: Topic Management

### 🔍 List Kafka Topics:
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### 📥 View Messages in a Kafka Topic:
```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic taxi-locations \
  --from-beginning \
  --max-messages 5
```

---

## 🚀 3. Flink: Deploying and Scheduling Jobs

> ⏱ **Run from one level under root** (i.e., inside `taxi_locations/`)

### 📤 Copy Compiled JAR to Flink JobManager:
```bash
docker cp consumer/taxi_flink/target/taxi_locations-1.jar flink-jobmanager:/opt/flink
```

### 🏃 Submit Job to Flink:
```bash
docker exec flink-jobmanager flink run -d /opt/flink/taxi_locations-1.jar
```

---

## 🧠 4. Redis: Inspecting Metrics and Location Data

### 🔌 Start Redis CLI:
```bash
docker exec -it redis redis-cli
```

### 🔑 View All Keys:
```redis
KEYS *
```

### 📊 View Metrics:
```redis
HGETALL metrics:speed
HGETALL metrics:avgSpeed
HGETALL metrics:distance
```

### 📍 View Specific Location Data:
```redis
HGETALL location:534
HGETALL location:8717
HGETALL location:7630
HGETALL location:6211
```

---
## 🖥️ 5. AWS EC2: Commands ( Amazon Linux 2; EOL:2026-06-30)

### 🔑 Connect to EC2 Instance
```bash
ssh -i <key.pem> ec2-user@<ec2-public-dns>
```

### 🛠️ Update Server & Install Essentials

#### Update package lists and upgrade
```bash
sudo yum update -y
```

#### Install Docker
```bash
sudo amazon-linux-extras install docker -y
sudo service docker start
sudo usermod -aG docker ec2-user
```

#### Install Docker Compose
```bash
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
docker-compose --version
```

#### Install Git
```bash
sudo yum install git -y
```

#### Install OpenJDK 17
```bash
sudo yum install java-17-amazon-corretto -y
java -version
```

#### (Optional) Install Maven
```bash
sudo yum install maven -y
mvn -version
```

### 🐳 Prune Unused Docker Resources
```bash
docker system prune -af
```
