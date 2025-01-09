#!/bin/bash
export FLASK_APP=hmpps_person_match
export FLASK_ENV=development
poetry run flask init-db
