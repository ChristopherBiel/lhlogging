FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py booking_model.py chassis.py chassis.html fleet_sim.py leg_stats.py monitor_stats.py ./
# Faceplate, vendored (dist/ with its MANIFEST, fonts.css with fonts/), and the
# project's own stylesheets, glyph, favicon and figure scripts. The CSS and the
# chassis live in real files so `npx faceplate check` can actually read them --
# it only scans .css/.html/.svg.
# Only what the pages use, not the whole vendored package: the two stylesheets,
# the mark of the footer signature (chassis.py inlines it) and the web fonts
# with their licences. tests/test_chassis.py holds this list against what
# chassis.py reads and serves.
COPY dist/faceplate.tokens.css dist/faceplate.components.css ./dist/
COPY dist/assets/logo/mark.svg ./dist/assets/logo/
COPY fonts.css ./
COPY fonts/*.woff2 fonts/OFL-*.txt ./fonts/
COPY static/ ./static/

EXPOSE 8080

CMD ["python", "app.py"]
