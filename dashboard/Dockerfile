FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
# Vendored Faceplate design system (tokens + components + MANIFEST) and the
# stylesheets, fonts and favicon the pages link. The CSS lives in real files so
# `npx faceplate check` can actually read it -- it only scans .css/.html/.svg.
# Only the two stylesheets, not the whole vendored dist: the package also ships
# the personal-layer logo assets, which have no business in this image.
COPY dist/faceplate.tokens.css dist/faceplate.components.css ./dist/
COPY static/ ./static/

EXPOSE 8080

CMD ["python", "app.py"]
