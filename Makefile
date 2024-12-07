build:
	git --no-pager tag | tail -n 1 | xargs -I % poetry version %
	poetry version --short > cta/_version
	poetry build
	pip install dist/*.tar.gz

create-dev:
	pre-commit install
	rm -rf env
	python3.10 -m venv env
	( \
		. env/bin/activate; \
		pip install -r requirements.txt; \
		poetry install; \
		deactivate; \
	)

package:
	pyinstaller --clean \
		--onefile \
		--add-data ./cta/_version:. \
		--workpath ./pyinstaller \
		--name src \
		--hidden-import cta \
		cta/main.py
