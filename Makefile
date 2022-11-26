install:
	poetry config virtualenvs.in-project
	poetry install --sync

test:
	poetry run pytest -rA -s --cov=src tests/

package:
	true