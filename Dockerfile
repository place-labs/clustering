FROM crystallang/crystal:0.32.1

WORKDIR /app

# Add
# - curl

# Install shards for caching
COPY shard.yml shard.yml
COPY shard.lock shard.lock

RUN shards install --production

# Add src
COPY ./src /app/src

# Build application
RUN crystal build /app/src/poc.cr

# Run the app binding on port 3000
CMD ["/app/poc"]
