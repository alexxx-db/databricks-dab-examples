import logging

logger = logging.getLogger(__name__)


def append_to_delta(df, dest_table, streaming=False, checkpoint_location=None):
    if not streaming:
        logger.info(f"Writing batch data to {dest_table}")
        df.write.format("delta").mode("append").saveAsTable(dest_table)
    else:
        if not checkpoint_location:
            raise ValueError("checkpoint_location is required when streaming=True")
        logger.info(f"Writing streaming data to {dest_table} with checkpoint {checkpoint_location}")
        df.writeStream.format("delta").outputMode("append").option("checkpointLocation", checkpoint_location).toTable(dest_table)
