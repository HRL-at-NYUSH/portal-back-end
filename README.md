Interactive Portal Backend

## How to launch the backend
First, you need to download [data](https://drive.google.com/file/d/1BjKVB6EEEqoDzM-IX-P_FX35Bmb2Ye9w/view?usp=sharing) and put it under `data/`.

### Without docker
1. Install dependencies: `pip install -r requirements.txt`.
2. Start server: `sanic server.app`. Use `--dev` for debug purpose.

### With docker
1. Build image with `docker build -t portal-back-end .`.
2. Start the service with `docker run -p 8000:8000 portal-back-end`.

## Features

1. Grouping by single variable.
2. Filtering by multiple ranges.

## Charts

- Bar chart
- Line chart with mean
- Line chart with count
- Area chart

## TODOs

1. Update histogram.
2. Add boxplot.
3. Add Scatterplot.
   These graphs cannot be directly transmitted because the data is too large.
