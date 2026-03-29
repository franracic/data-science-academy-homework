## Pokrenite (u direktoriju hw4):

docker-compose up -d

## Pristup

- ClickHouse
  - port 8123, dok se programi spajaju preko porta 9000.
  - Username: default
  - Password: admin

- Grafana
  - adresa: http://localhost:3000
  - username: admin, password: admin
  - spojiti clickHouse u grafanu putem connections -> data sources -> clickHouse

## Zaustavljanje

docker-compose down
