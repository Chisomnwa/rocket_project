import json
import requests

# write a function to download all pictures in the launches.json file
def _get_pictures():
    # Load the JSON data
    with open("/opt/airflow/dags/rockets/launches/launches.json") as f:
        launches = json.load(f)

        # Initialize a list to store image URLs
        image_urls = []

        # Extract image URLs from the JSON data
        for launch in launches["results"]:
            image_urls.append(launch["image"])

        # Download each image from the URLs
        for image_url in image_urls:
            response = requests.get(image_url)
            image_filename = image_url.split("/")[-1]
            target_file = f"/opt/airflow/dags/rockets/images/{image_filename}"

            with open(target_file, "wb") as f:
                f.write(response.content)
            print(f"Download {image_url} to {target_file} successful")
