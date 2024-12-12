# Receiving messages and saving it in bigtable
# Think of it as scheduling a job, taking in new messages from 
# pubsub, transforming it and then saving it in Bigtable.

from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1, bigquery
import json

project_id = "regal-habitat-444222-u9"
subscription_id = "<sub-id>"
bq_dataset_id = "DE_practice"
bq_table_id = "floods-table"

# Number of seconds the subscriber should listen for messages
timeout = 15.0
sub_message = []

subscriber = pubsub_v1.SubscriberClient()
bigquery_client = bigquery.Client()

# Full path to the Pub/Sub subscription
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """Callback to handle incoming Pub/Sub messages and write them to BigQuery."""
    try:
        # Decode the message data (assuming it's in JSON format)
        row = message.data.decode("utf-8") 
        row_dict = json.loads(row)
        lim = 350 if len(row_dict["message"]) >350 else len(row_dict["message"])
        row_dict["message"] = row_dict["message"][:lim]
        data = {}
        data["flood_id"] = row_dict["@id"]
        data["message"] = row_dict["message"]
        data["severity"] = row_dict["severity"]
        data["severityLevel"] = int(row_dict["severityLevel"])

        # Define the BigQuery table path
        table_ref = f"{project_id}.{bq_dataset_id}.{bq_table_id}"
        errors = bigquery_client.insert_rows_json(table_ref, [data])  # Rows must be a list of dictionaries
        
        if not errors:
            print("New row has been added to BigQuery.")
            message.ack()  # Acknowledge the message
        else:
            print(f"Errors occurred while writing to BigQuery: {errors}")
            message.nack()  # Do not acknowledge the message if there's an error
            
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  
        streaming_pull_future.result()
