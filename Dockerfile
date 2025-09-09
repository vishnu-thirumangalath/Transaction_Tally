FROM python:3.11-slim-bookworm

# Install Java + procps
RUN apt-get update && apt-get install -y openjdk-17-jdk procps && rm -rf /var/lib/apt/lists/*

# Detect JAVA_HOME dynamically
RUN JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "JAVA_HOME=$JAVA_HOME" >> /etc/environment && \
    echo "export JAVA_HOME=$JAVA_HOME" >> /etc/profile

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /app
COPY . .

EXPOSE 5000
ENTRYPOINT ["python"]
CMD ["run.py"]

