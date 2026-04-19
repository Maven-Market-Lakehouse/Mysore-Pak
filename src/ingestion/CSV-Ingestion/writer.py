def write_data(df, table, checkpoint):
    return (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(table)
    )