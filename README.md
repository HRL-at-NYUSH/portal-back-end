# portal-back-end

## How to run
1. Install dependencies: `pip install -r requirements.txt`.
2. Put [data](https://drive.google.com/file/d/1BjKVB6EEEqoDzM-IX-P_FX35Bmb2Ye9w/view?usp=sharing) under `data/`.
3. `cd src`.
4. Start server: `sanic server.app`. Use `--dev` for hot reloading.

## Features
1. Grouping by single variable.
2. Filtering by multiple ranges.

## Charts
* Bar chart
* Line chart with mean
* Line chart with count
* Area chart

## TODOs
1. Add histogram.
2. Add boxplot.
3. Add Scatterplot.
These graphs cannot be directly transmitted because the data is too large.
