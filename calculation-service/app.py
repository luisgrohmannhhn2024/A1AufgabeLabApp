import requests
import json


# Function to handle data aggregation and post request
def aggregate_and_send_data():
    # GET request to retrieve the events data from the assessment-service
    url = "http://assessment-service:8080/v1/dataset"
    response = requests.get(url)

    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Dictionary to calculate the total consumption for each customer
        dictionary_total = {}
        dictionary_for_calculation = {}

        # Process events
        for event in sorted(data['events'], key=lambda x: x['timestamp']):
            customer_id = event['customerId']
            workload_id = event['workloadId']
            timestamp = event['timestamp']
            event_type = event['eventType']

            # Initialize dictionary for customer if not present
            if customer_id not in dictionary_for_calculation:
                dictionary_for_calculation[customer_id] = {}

            # Handle start and stop events
            if event_type == "start":
                # Store start time
                dictionary_for_calculation[customer_id][workload_id] = timestamp
            elif event_type == "stop":
                # Ensure there's a start time before calculating the runtime
                if workload_id in dictionary_for_calculation[customer_id]:
                    start_time = dictionary_for_calculation[customer_id][workload_id]
                    runtime = timestamp - start_time

                    # Add runtime to the customer's total consumption
                    if customer_id in dictionary_total:
                        dictionary_total[customer_id] += runtime
                    else:
                        dictionary_total[customer_id] = runtime

                    # Remove the workload from ongoing calculations
                    del dictionary_for_calculation[customer_id][workload_id]

        # Prepare the result to be sent via POST request
        result_data = {
            "result": [
                {
                    "customerId": customer_id,
                    "consumption": total_time  # total_time is in milliseconds or your preferred unit
                }
                for customer_id, total_time in dictionary_total.items()
            ]
        }

        # POST request to send the aggregated data to the assessment-service
        post_url = "http://assessment-service:8080/v1/result"
        post_response = requests.post(post_url, json=result_data)

        # Check if the POST request was successful
        if post_response.status_code == 200:
            print(f"POST succeeded with status code {post_response.status_code}: {post_response.text}")
        else:
            print(f"POST request failed with status code {post_response.status_code}: {post_response.text}")

    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")


# Run the aggregation and send function
if __name__ == "__main__":
    aggregate_and_send_data()
