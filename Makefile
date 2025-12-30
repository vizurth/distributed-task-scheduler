build_and:
	docker-compose -f docker-compose.yaml build

up:
	docker-compose -f docker-compose.yaml up -d --build

down:
	docker-compose -f docker-compose.yaml down