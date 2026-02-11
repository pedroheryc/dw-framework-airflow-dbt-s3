.PHONY: up down logs ps bash-airflow bash-postgres

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

bash-airflow:
	docker compose exec airflow-webserver bash

bash-postgres:
	docker compose exec postgres bash
