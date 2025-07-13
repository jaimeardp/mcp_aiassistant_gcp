docker run --name fastmcp-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=secret \
  -e POSTGRES_DB=mydb \
  -p 5432:5432 \
  -v pgdata:/var/lib/postgresql/data \
  -d postgres:16-alpine
