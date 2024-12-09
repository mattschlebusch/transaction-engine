# Transaction Engine

Command-line interface that demonstrates batch processing using a batch of transaction requests
and produces a summary of the accounts.

The application is assumes a default/single asset class. The data schema can be modified to accommodate multiple assets.

## Data Schema

### Input

```csv
type, client, tx, amount
deposit, 1, 1, 1.0
withdrawal, 2, 5, 3.0
```

### Output

```csv
client, available, held, total, locked
1, 1.5, 0.0, 1.5, false
2, 2.0, 0.0, 2.0, false
```

