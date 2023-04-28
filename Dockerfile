# Install the base requirements for the app.
# This stage is to support development.
FROM --platform=$BUILDPLATFORM timescale/timescaledb:latest-pg13 as base

LABEL maintainer="Douglas Martins <douglas.capelossi@gmail.com>"

WORKDIR /app
ADD . /app
RUN apk add --update py-pip
RUN apk add --update yarn

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN yarn install --production
CMD ["node", "src/index.js"]
EXPOSE 3000