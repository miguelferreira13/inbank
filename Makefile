start-sqlbrowser:
	docker run -d \
	--name=sqlitebrowser \
	-v ./netflix.db:/config/netflix.db \
	-p 3000:3000 \
	--rm lscr.io/linuxserver/sqlitebrowser

stop-sqlbrowser:
	docker stop sqlitebrowser

run:
	python3 src/main.py

