install:
	poetry config virtualenvs.in-project
	poetry install --sync

test:
	poetry run pytest --cov=src tests/

package:
	true