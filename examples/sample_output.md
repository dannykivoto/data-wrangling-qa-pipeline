# Expected output

After running the pipeline you should see:

- one accepted batch (`batch_001`)
- one rejected batch (`batch_002`)
- cleaned parquet output under `data/processed/`
- rejected batch quarantined under `data/quarantine/`
- lineage and validation files under `data/lineage/`
