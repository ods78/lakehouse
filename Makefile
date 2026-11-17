# LakeHouse control shortcuts — the canonical interface is ./lakehouse.sh
.PHONY: init up down status logs build smoke backup reset

init:
	./lakehouse.sh init

up:
	./lakehouse.sh up all

down:
	./lakehouse.sh down all

status:
	./lakehouse.sh status

logs:
	./lakehouse.sh logs

build:
	./lakehouse.sh build

smoke:
	./lakehouse.sh smoke

backup:
	./lakehouse.sh backup

reset:
	./lakehouse.sh reset
