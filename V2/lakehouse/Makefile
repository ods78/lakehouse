.RECIPEPREFIX := >
NETS := lh_sources lh_ingestion lh_storage lh_processing lh_serving lh_consumers lh_ops
.PHONY: env networks p0 p1 p2 p3 ps down verify-p1 verify-p2 verify-p3
env:
> @test -f .env || cp .env.example .env
networks:
> @for n in $(NETS); do docker network inspect $$n >/dev/null 2>&1 || docker network create $$n; done
p0: env networks
> docker compose up -d postgres
p1: p0
> docker compose --profile sources up -d
p2: p0
> docker compose --profile ingestion up -d
p3: p0
> docker compose --profile storage up -d
ps:
> docker compose --profile "*" ps
down:
> docker compose --profile "*" down
verify-p1:
> ./scripts/verify-p1.sh
verify-p2:
> ./scripts/verify-p2.sh
verify-p3:
> ./scripts/verify-p3.sh
