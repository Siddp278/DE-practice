from google.cloud import pubsub_v1
from typing import Callable
from concurrent import futures
import json
import sys
from google.cloud.storage import Client

project_id = "<project-id>"
topic_id = "<topic-name>"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
publish_futures = []

client = Client()
bucket = client.get_bucket("<bicket-name>")
blob = bucket.get_blob("<file-location>")
record = json.loads(blob.download_as_text(encoding="utf-8"))

def get_callback(
    publish_future: pubsub_v1.publisher.futures.Future, data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback

for i, data in enumerate(record["items"]):
    if i>int(sys.argv[1]) and i<int(sys.argv[2]):
        publish_future = publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
        # Non-blocking. Publish failures are handled in the callback function.
        publish_future.add_done_callback(get_callback(publish_future, data))
        publish_futures.append(publish_future)
    elif i>=int(sys.argv[2]):
        break

# Wait for all the publish futures to resolve before exiting.
futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

print(f"Published messages with error handler to {topic_path}.")