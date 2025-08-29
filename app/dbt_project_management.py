import json
from app.config import SourceConfig
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_dbt_project_model_count(name):
    env_variables = SourceConfig.get_environment_variables()
    url_base = env_variables.get('dbt_init_api_link')
    url = f"{url_base}/api/v3/projects/{name}/models"
    headers = {
        "Accept": "application/json",
        "X-API-KEY": env_variables.get('dbt_init_api_key')
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        models_count = data["models_count"]

        return models_count
    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}")
    except ValueError as json_err:
        print(f"JSON decoding error: {json_err}")

    return None

def get_dbt_project_seed_count(name):
    env_variables = SourceConfig.get_environment_variables()
    url_base = env_variables.get('dbt_init_api_link')
    url = f"{url_base}/api/v3/projects/{name}/seeds"
    headers = {
        "Accept": "application/json",
        "X-API-KEY": env_variables.get('dbt_init_api_key')
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        seeds_count = data["seeds_count"]

        return seeds_count
    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}")
    except ValueError as json_err:
        print(f"JSON decoding error: {json_err}")

    return None

def get_dbt_project_snapshot_count(name):
    env_variables = SourceConfig.get_environment_variables()
    url_base = env_variables.get('dbt_init_api_link')
    url = f"{url_base}/api/v3/projects/{name}/snapshots"
    headers = {
        "Accept": "application/json",
        "X-API-KEY": env_variables.get('dbt_init_api_key')
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        shapshots_count = data["snapshots_count"]

        return shapshots_count
    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}")
    except ValueError as json_err:
        print(f"JSON decoding error: {json_err}")

    return None

def get_dbt_project_source_count(name):
    env_variables = SourceConfig.get_environment_variables()
    url_base = env_variables.get('dbt_init_api_link')
    url = f"{url_base}/api/v3/projects/{name}/sources"
    headers = {
        "Accept": "application/json",
        "X-API-KEY": env_variables.get('dbt_init_api_key')
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        sources_count = data["sources_count"]

        return sources_count
    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}")
    except ValueError as json_err:
        print(f"JSON decoding error: {json_err}")

    return None

def get_dbt_project_task_count(name):
    env_variables = SourceConfig.get_environment_variables()
    url_base = env_variables.get('dbt_init_api_link')
    url = f"{url_base}/api/v3/projects/{name}/tasks"
    headers = {
        "Accept": "application/json",
        "X-API-KEY": env_variables.get('dbt_init_api_key')
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        tasks = data["tasks"]

        return tasks
    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}")
    except ValueError as json_err:
        print(f"JSON decoding error: {json_err}")

    return {}

def get_dbt_project_test_count(name):
    env_variables = SourceConfig.get_environment_variables()
    url_base = env_variables.get('dbt_init_api_link')
    url = f"{url_base}/api/v3/projects/{name}/tests"
    headers = {
        "Accept": "application/json",
        "X-API-KEY": env_variables.get('dbt_init_api_key')
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        test_count = data["test_count"]

        return test_count
    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}")
    except ValueError as json_err:
        print(f"JSON decoding error: {json_err}")

    return None

def get_dbt_project_variables(name):
    env_variables = SourceConfig.get_environment_variables()
    url_base = env_variables.get('dbt_init_api_link')
    url = f"{url_base}/api/v3/projects/{name}/variables"
    headers = {
        "Accept": "application/json",
        "X-API-KEY": env_variables.get('dbt_init_api_key')
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        key, value = next(iter(data['variables'].items()))

        return value
    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}")
    except ValueError as json_err:
        print(f"JSON decoding error: {json_err}")

    return None

def get_dbt_project_status(name):
    env_variables = SourceConfig.get_environment_variables()
    url_base = env_variables.get('dbt_init_api_link')
    url = f"{url_base}/api/v3/projects/{name}/status"
    headers = {
        "Accept": "application/json",
        "X-API-KEY": env_variables.get('dbt_init_api_key')
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        return data
    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}")
    except ValueError as json_err:
        print(f"JSON decoding error: {json_err}")

    return None

def get_dbt_project_owner(name):
    env_variables = SourceConfig.get_environment_variables()
    url_base = env_variables.get('dbt_init_api_link')
    url = f"{url_base}/api/v3/projects/{name}/owner"
    headers = {
        "Accept": "application/json",
        "X-API-KEY": env_variables.get('dbt_init_api_key')
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        return response.json()
    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}")
    except ValueError as json_err:
        print(f"JSON decoding error: {json_err}")

    return {}

def get_dbt_project_info(name):
    env_variables = SourceConfig.get_environment_variables()
    url_base = env_variables.get('dbt_init_api_link')
    url = f"{url_base}/api/v3/projects/{name}/info"
    headers = {
        "Accept": "application/json",
        "X-API-KEY": env_variables.get('dbt_init_api_key')
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        return response.json()
    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}")
    except ValueError as json_err:
        print(f"JSON decoding error: {json_err}")

    return {}


def process_project(project):
    """
    Function to process a single project by fetching variables, owner, and tasks.
    """
    project_name = project["project_name"]
    project["status"] = get_dbt_project_status(project_name)
    owner = get_dbt_project_owner(project_name)
    project.update(owner)

    return project

def get_dbt_projects():
    env_variables = SourceConfig.get_environment_variables()
    url_base = env_variables.get('dbt_init_api_link')
    url = f"{url_base}/api/v3/projects"
    headers = {
        "Accept": "application/json",
        "X-API-KEY": env_variables.get('dbt_init_api_key')
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        projects = response.json()

        # Use ThreadPoolExecutor for concurrent I/O tasks
        with ThreadPoolExecutor() as executor:
            future_to_project = {executor.submit(process_project, project): project for project in projects}

            processed_projects = []
            for future in as_completed(future_to_project):
                try:
                    processed_project = future.result()
                    processed_projects.append(processed_project)
                except Exception as e:
                    print(f"Error processing project: {e}")

        processed_projects.sort(key=lambda project: project["project_name"])

        return json.dumps(processed_projects)
    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}")
    except ValueError as json_err:
        print(f"JSON decoding error: {json_err}")

    return None
