PHONY: start-sqlbrowser stop-sqlbrowser clean-db run help require-docker
.DEFAULT_GOAL := help

start-sqlbrowser: ## Start the sqlitebrowser container
	@echo "Starting sqlitebrowser"
	docker run -d \
	--name=sqlitebrowser \
	-v ./netflix.db:/config/netflix.db \
	-p 3000:3000 \
	--rm lscr.io/linuxserver/sqlitebrowser

stop-sqlbrowser: ## Stop the sqlitebrowser container
	@echo "Stopping sqlitebrowser"
	docker stop sqlitebrowser

clean-db: ## Remove the netflix.db file
	@echo "Removing netflix.db"
	rm -f netflix.db

run: clean-db ## Run the ETL process
	@echo "Running ETL process..."
	python3 netflix_etl/main.py

help: ## Display this help section
	@echo "$$(grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}')"

require-docker: ## Check if docker is installed
	@which docker > /dev/null || \
		(echo "Docker not found.\nPlease install docker and try again." && exit 1)
