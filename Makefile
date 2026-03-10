# d before commands stands for docker
drebuild:
	docker compose build && docker compose down && docker compose up -d

dlogs:
	docker compose logs -f

ddown:
	docker compose down

dup:
	docker compose up -d --build

dbuild:
	docker compose build

drestart:
	docker compose restart

dclean:
	docker builder prune --all --force
