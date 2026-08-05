import pg from "pg";

// One shared way to reach Postgres (started by docker-compose on host port
// 5433). Like the Kafka producer: one pool per process, created at startup.
export function createPool() {
  return new pg.Pool({
    host: "localhost",
    port: 5433,
    user: "postgres",
    password: "kafka",
    database: "chat",
  });
}
