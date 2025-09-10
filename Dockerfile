FROM python:3.11-slim-bookworm

# Install Java + procps
RUN apt-get update && apt-get install -y openjdk-17-jdk procps && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME to Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Persist in ENV
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /app
COPY . .

EXPOSE 5000
ENTRYPOINT ["python"]
CMD ["run.py"]
