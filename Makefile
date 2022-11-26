install:
	poetry config virtualenvs.in-project
	poetry install --sync

test:
	poetry run pytest -rA --cov=src -m "unit or functional or e2e" tests/

unit:
	poetry run pytest -rA --cov=src -m unit tests/

functional:
	pytest -rA --cov=src -m functional tests/

e2e:
	pytest -rA --cov=src -m e2e tests/

package:
	echo notimplemented

run:
	rm -rf ./run
	sh run.sh