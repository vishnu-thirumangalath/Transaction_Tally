FROM python:3.11-slim-bookworm

# Install Java + procps
RUN apt-get update && apt-get install -y openjdk-17-jdk procps && rm -rf /var/lib/apt/lists/*

ARG TARGETPLATFORM
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
      ln -s /usr/lib/jvm/java-17-openjdk-arm64 /usr/lib/jvm/default-java; \
    else \
      ln -s /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/default-java; \
    fi

# Set JAVA_HOME & PATH globally
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:${PATH}"



# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /app
COPY . .

EXPOSE 5000
ENTRYPOINT ["python"]
CMD ["run.py"]
