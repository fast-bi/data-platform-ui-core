## Disabled libraries
# import sys
# import base64
# from datetime import timedelta
# from flask_login import UserMixin
# from classes import StreamToLogger
## Enabled libraries
import os
import logging
import requests
import jwt
import time
import json
import urllib.parse
from urllib.parse import unquote
from datetime import datetime
from flask import Flask, redirect, url_for, render_template, flash, session, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename
from flask_login import LoginManager, login_user, logout_user, current_user
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from flask_oidc import OpenIDConnect
from flask_session import Session
from flask_mail import Mail
from flask_caching import Cache
from app.config import Config, SourceConfig
from app.classes import User, CustomUser
from app.user_console_db import UserConsoleMetadataHandler
from packaging import version
from concurrent.futures import ThreadPoolExecutor

import app.dbt_project_management as dpm

import random

# Determine the directory based on the environment
if Config.FLASK_ENV == "production":
    template_dir = os.path.abspath('./fastbi-platform/')
    static_dir = os.path.abspath('./fastbi-platform/')
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    def test_get_environment_variables():
        env_vars = SourceConfig.get_environment_variables()
        for key, value in env_vars.items():
            assert value is not None, f"{key} should not be None"
    test_get_environment_variables()
else:
    template_dir = os.path.abspath('./fastbi-platform/')
    static_dir = os.path.abspath('./fastbi-platform/')
    def test_get_environment_variables():
        env_vars = SourceConfig.get_environment_variables()
        for key, value in env_vars.items():
            assert value is not None, f"{key} should not be None"
    test_get_environment_variables()
    # Set up logging
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

# Create the Flask User Console application
app = Flask(__name__, template_folder=template_dir, static_url_path="/", static_folder=template_dir )
# Get app Configuration
app.config.from_object(Config)
# Set Database configuration
db_config = {
    'dbname': app.config['DB_NAME'],
    'user': app.config['DB_USER'],
    'password': app.config['DB_PASSWORD'],
    'host': app.config['DB_HOST'],
    'port': app.config['DB_PORT']
}

CORS(app)
Session(app)
mail = Mail(app)
oidc = OpenIDConnect(app)
login = LoginManager(app)
login.login_view = 'index'

user_metadata_handler = UserConsoleMetadataHandler(db_config)
app.user_metadata_handler = user_metadata_handler

# Configure caching with Redis using parameters from Config
app.cache = Cache(app)

def get_bq_stats():
    # Import bigquery_stats only when needed
    import app.datawarehouse_stats.bigquery_stats as bq
    with ThreadPoolExecutor() as executor:
        futures = {
            # cards
            'dataset_count': executor.submit(bq.get_dataset_count),
            'total_query_executed': executor.submit(bq.get_total_query_executed),
            'table_count': executor.submit(bq.get_table_count),
            'avg_execution_time_seconds': executor.submit(bq.get_avg_execution_time_seconds),
            'failure_rate_percentage': executor.submit(bq.get_failure_rate_percentage),
            # charts
            'query_cost_by_months_chart': executor.submit(bq.get_query_cost_by_month),
            'query_cost_by_days_chart': executor.submit(bq.get_query_cost_for_last_30_days),
            # tables
            'total_cost_gb_by_users': executor.submit(bq.get_total_cost_gb_by_users),
            'total_cost_gb_by_table': executor.submit(bq.get_total_cost_gb_by_table)
        }
        results = {key: future.result() for key, future in futures.items()}
    return results

def get_sf_stats():
    # Import snowflake_stats only when needed
    import app.datawarehouse_stats.snowflake_stats as sf
    with ThreadPoolExecutor() as executor:
        futures = {
            # cards
            'dataset_count': executor.submit(sf.get_dataset_count),
            'total_query_executed': executor.submit(sf.get_total_query_executed),
            'table_count': executor.submit(sf.get_table_count),
            'avg_execution_time_seconds': executor.submit(sf.get_avg_execution_time_seconds),
            'failure_rate_percentage': executor.submit(sf.get_failure_rate_percentage),
            # charts
            'query_cost_by_months_chart': executor.submit(sf.get_query_cost_by_month),
            'query_cost_by_days_chart': executor.submit(sf.get_query_cost_for_last_30_days),
            # tables
            'total_cost_gb_by_users': executor.submit(sf.get_total_cost_gb_by_users),
            'total_cost_gb_by_table': executor.submit(sf.get_total_cost_gb_by_table)
        }
        results = {key: future.result() for key, future in futures.items()}
    return results

def get_rd_stats():
    # Import redshift_stats only when needed
    import app.datawarehouse_stats.redshift_stats as rd
    with ThreadPoolExecutor() as executor:
        futures = {
            # cards
            'dataset_count': executor.submit(rd.get_dataset_count),
            'total_query_executed': executor.submit(rd.get_total_query_executed),
            'table_count': executor.submit(rd.get_table_count),
            'avg_execution_time_seconds': executor.submit(rd.get_avg_execution_time_seconds),
            'failure_rate_percentage': executor.submit(rd.get_failure_rate_percentage),
            # charts
            'query_cost_by_months_chart': executor.submit(rd.get_query_cost_by_month),
            'query_cost_by_days_chart': executor.submit(rd.get_query_cost_for_last_30_days),
            # tables
            'total_cost_gb_by_users': executor.submit(rd.get_total_cost_gb_by_users),
            'total_cost_gb_by_table': executor.submit(rd.get_total_cost_gb_by_table)
        }
        results = {key: future.result() for key, future in futures.items()}
    return results

def get_ft_stats():
    # Import fabric_stats only when needed
    import app.datawarehouse_stats.fabric_stats as ft
    with ThreadPoolExecutor() as executor:
        futures = {
            # cards
            'dataset_count': executor.submit(ft.get_dataset_count),
            'total_query_executed': executor.submit(ft.get_total_query_executed),
            'table_count': executor.submit(ft.get_table_count),
            'avg_execution_time_seconds': executor.submit(ft.get_avg_execution_time_seconds),
            'failure_rate_percentage': executor.submit(ft.get_failure_rate_percentage),
            # charts
            'query_cost_by_months_chart': executor.submit(ft.get_query_cost_by_month),
            'query_cost_by_days_chart': executor.submit(ft.get_query_cost_for_last_30_days),
            # tables
            'total_cost_gb_by_users': executor.submit(ft.get_total_cost_gb_by_users),
            'total_cost_gb_by_table': executor.submit(ft.get_total_cost_gb_by_table)
        }
        results = {key: future.result() for key, future in futures.items()}
    return results

def get_airbyte_workspace_id():
    """
    Get the first workspace ID using the new Airbyte API format.
    New API: GET /v1/workspaces
    """
    url_base = app.config['AIRBYTE_API_LINK']
    api_base = app.config['AIRBYTE_API_BASE']
    api_version = "v1"
    url = f"{url_base}/{api_base}/{api_version}/workspaces"
    headers = {"accept": "application/json"}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None  # Return None if there's a connection error or HTTP error

    try:
        data = response.json()
        # New API returns data directly, not wrapped in "workspaces" key
        workspaces = data.get("data", []) if isinstance(data, dict) else data
        if workspaces and len(workspaces) > 0:
            return workspaces[0]["workspaceId"]  # Assuming you want the ID of the first workspace
    except ValueError as ve:
        print(f"Error decoding JSON: {ve}")

    return None  # Return None if the workspace ID is not found or if there's an error in decoding JSON


def get_airbyte_destination_image(destination_definition_id, workspace_id):
    """
    Fetch the docker image tag for a given Airbyte destination_definition_id.
    New API: GET /v1/workspaces/{workspaceId}/definitions/destinations/{definitionId}

    Args:
        destination_definition_id: The destination definition ID
        workspace_id: The workspace ID (required for new API)

    Returns:
        dict: The destination definition data if successful, None otherwise.
    """
    if not workspace_id:
        print("Error: workspace_id is required for new Airbyte API")
        return None
        
    url_base = app.config['AIRBYTE_API_LINK']
    api_base = app.config['AIRBYTE_API_BASE']
    api_version = "v1"
    url = f"{url_base}/{api_base}/{api_version}/workspaces/{workspace_id}/definitions/destinations/{destination_definition_id}"
    headers = {"accept": "application/json"}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching destination image: {e}")
        return None

    if response.status_code == 200:
        return response.json()
    return None


def get_airbyte_destination_type(destination_id, workspace_id):
    """
    Determine the type of Airbyte destination based on docker_image_tag.

    Args:
        destination_id: The destination ID
        workspace_id: The workspace ID (required for new API)

    Returns:
        tuple: (bigquery_version, destination_name, dwh_type) or (None, None, None)
    """
    if destination_id is None:
        return None, None, None

    url_base = app.config['AIRBYTE_API_LINK']
    api_base = app.config['AIRBYTE_API_BASE']
    api_version = "v1"
    url = f"{url_base}/{api_base}/{api_version}/destinations/{destination_id}"
    headers = {"accept": "application/json"}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching destination type: {e}")
        return None, None, None

    if response.status_code == 200:
        destination_details = response.json()
        destination_definition_id = destination_details.get("definitionId", "")
        # Get the destination name from destination details (this is the instance name like "BigQuery_Xxi_Destination")
        destination_instance_name = destination_details.get("name", "")
        
        # Get destination definition data to get the proper destination name (like "BigQuery")
        image_api = get_airbyte_destination_image(destination_definition_id, workspace_id)
        docker_image_tag = image_api.get("dockerImageTag", "") if image_api else ""
        docker_repo = image_api.get("dockerRepository", "") if image_api else ""
        # Get the proper destination name from the definition
        destination_name = image_api.get("name", "") if image_api else ""
        
        dwh_type = None
        if docker_repo == 'airbyte/destination-bigquery':
            dwh_type = 'bigquery'
        elif docker_repo == 'airbyte/destination-snowflake':
            dwh_type = 'snowflake'
        elif docker_repo == 'airbyte/destination-redshift':
            dwh_type = 'redshift'
        elif docker_repo == 'airbyte/destination-fabric':
            dwh_type = 'fabric'

        bigquery_version = None
        if docker_image_tag:
            parsed_docker_image_tag = version.parse(docker_image_tag)
            last_v1_docker_image_tag = version.parse('1.10.2')  # last possible version for v1 destination Bigquery
            if parsed_docker_image_tag > last_v1_docker_image_tag:
                bigquery_version = 2
            else:
                bigquery_version = 1
        return bigquery_version, destination_name, dwh_type
    return None, None, None


def get_airbyte_connections(workspace_id):
    """
    Get all connections using the new Airbyte API format.
    New API: GET /v1/connections
    Handles pagination to fetch all connections, not just the first page.
    """
    if workspace_id is None:
        return {
            "None - Basic dbt Labs Project Initialization": {
                "connection": "None",
                "destination_version": "None",
                "destination_name": "None",
                "destination_type": "None"
            }
        }

    url_base = app.config['AIRBYTE_API_LINK']
    api_base = app.config['AIRBYTE_API_BASE']
    api_version = "v1"
    base_url = f"{url_base}/{api_base}/{api_version}/connections"
    headers = {"accept": "application/json"}

    result = {
        "None - Basic dbt Labs Project Initialization": {
            "connection": "None",
            "destination_version": "None",
            "destination_name": "None",
            "destination_type": "None"
        }
    }

    # Pagination parameters
    limit = 100  # Request up to 100 items per page
    offset = 0
    has_more = True

    while has_more:
        # Build URL with pagination parameters
        params = {
            "limit": limit,
            "offset": offset
        }
        url = f"{base_url}?{urllib.parse.urlencode(params)}"

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching Airbyte connections: {e}")
            # If this is the first page and it fails, return the default "None" option
            if offset == 0:
                return {
                    "None - Basic dbt Labs Project Initialization": {
                        "connection": "None",
                        "destination_version": "None",
                        "destination_name": "None",
                        "destination_type": "None"
                    }
                }
            # If a subsequent page fails, break and return what we have
            break

        if response.status_code == 200:
            response_json = response.json()
            # New API returns data in a "data" array
            connections_data = response_json.get("data", [])
            
            # Process connections from this page
            for connection in connections_data:
                connection_id = connection.get("connectionId")
                destination_id = connection.get("destinationId")
                init_version, destination_name, dwh_type = get_airbyte_destination_type(destination_id, workspace_id)
                name = connection.get("name")

                result[name] = {
                    "connection": connection_id,
                    "destination_id": destination_id,  # Store the destination ID for later use
                    "destination_version": init_version,
                    "destination_name": destination_name,
                    "destination_type": dwh_type
                }

            # Check if there are more pages
            # Airbyte API v1 returns pagination info in a "next" field with a URL
            # Priority: Check "next" field first (most reliable indicator)
            
            next_url = response_json.get("next")
            pagination = response_json.get("pagination", {})
            has_more_pagination = pagination.get("hasMore", False)
            
            # If there's a "next" URL, there are more pages - extract offset from it
            if next_url:
                # Extract offset from next URL (e.g., "http://...?limit=20&offset=20")
                if isinstance(next_url, str) and "offset" in next_url:
                    try:
                        parsed = urllib.parse.urlparse(next_url)
                        query_params = urllib.parse.parse_qs(parsed.query)
                        offset = int(query_params.get("offset", [offset + limit])[0])
                        # Note: API may enforce its own limit (e.g., 20), but we keep our requested limit
                        # The offset extraction ensures we continue from the correct position
                    except (ValueError, KeyError, IndexError) as e:
                        print(f"Warning: Could not parse offset from next URL: {e}")
                        offset += limit
                else:
                    # If next is a simple token/number, increment offset
                    offset += limit
            # If pagination object indicates more pages
            elif has_more_pagination:
                offset += limit
            # If we got fewer items than requested, we've reached the last page
            elif len(connections_data) < limit:
                has_more = False
            # If we got exactly the limit, there might be more, so continue
            elif len(connections_data) == limit:
                offset += limit
                # Safety check: prevent infinite loops by setting a maximum offset
                # Assuming reasonable maximum of 10,000 connections (100 pages * 100 items)
                if offset > 10000:
                    print("Warning: Reached maximum pagination limit (10,000 connections). Some connections may be missing.")
                    has_more = False
            else:
                # No more pages
                has_more = False
        else:
            # Non-200 status, stop pagination
            has_more = False

    return result

def get_jwt_token(user_name, user_email):
    key_path = app.config['GRAFANA_JWT_KEY_PATH']

    try:
        with open(key_path, 'r') as key_file:
            private_key = key_file.read()
    except FileNotFoundError:
        print(f"Key file not found at {key_path}")
        return None

    payload = {
        "sub": user_email,
        "email": user_email,
        "name": user_name,
        "iat": int(time.time()),
        "org_id": 1,
        "role": "Admin",
        "exp": int(time.time() + 3600)
    }

    try:
        token = jwt.encode(payload, private_key, algorithm='RS256')
        return token
    except jwt.exceptions.PyJWTError as e:
        print(f"JWT encoding error: {e}")
        return None

# This function might be called after successful OIDC authentication
def handle_user_login(user_info):
    user_email = user_info['email']
    username = user_info.get('preferred_username', user_email.split('@')[0])
    user = app.user_metadata_handler.get_user_by_email(user_email)

    if not user:
        user_id = app.user_metadata_handler.add_user(username, user_email)
        user = {'id': user_id, 'username': username, 'email': user_email}
    else:
        user_id = user['id']
        app.user_metadata_handler.update_login_time(user_id)

    # Create a user instance for Flask-Login
    user_instance = CustomUser(user_id, username, user_email)
    login_user(user_instance, remember=True)

@login.user_loader
def load_user(user_id):
    # Assuming you have a way to get user data as a list from a database
    user_info = app.user_metadata_handler.get_user_by_id(user_id)
    if user_info:
        return User(*user_info)  # Unpack the list into the User constructor
    return None

@app.route('/login')
def login():
    if not oidc.user_loggedin:
        return oidc.redirect_to_auth_server()
    
    user_info = oidc.user_getinfo(['email', 'preferred_username'])
    user_email = user_info['email']
    username = user_info.get('preferred_username', user_email.split('@')[0])

    user_data = app.user_metadata_handler.get_user_by_email(user_email)
    if not user_data:
        user_id = app.user_metadata_handler.add_user(username, user_email)
        user = CustomUser(user_id, username, user_email)
    else:
        user_id = user_data['id']
        user = CustomUser(user_id, user_data['username'], user_data['email'])
        app.user_metadata_handler.update_login_time(user_id)

        # Debugging: Check user_data contents
        print(f"User Data: {user_data}")

        # Set session parameters
        session['follow_mode'] = user_data.get('follow_mode', 'default_follow_mode')  # Set a default if not found
        session['iframe_mode'] = user_data.get('iframe_mode', 'default_iframe_mode')  # Set a default if not found
        session['light_dark_mode'] = user_data.get('light_dark_mode', 'default_light_dark_mode')  # Set a default if not found

        # Debugging: Check session values
        print(f"Session Values: follow_mode={session['follow_mode']}, iframe_mode={session['iframe_mode']}, light_dark_mode={session['light_dark_mode']}")

    login_user(user, remember=True)
    return redirect(url_for('index'))

@app.route('/signout')
def logout():
    """Log out the user and perform a redirect to OIDC logout."""
    oidc_auth_info = session.get('oidc_auth_token')
    if oidc_auth_info:
        id_token = oidc_auth_info.get('id_token')
        if id_token:
            # Construct the post_logout_redirect_uri
            post_logout_redirect_uri = urllib.parse.quote(url_for('index', _external=True), safe='')
            logout_url = f"{oidc.client_secrets['issuer']}/protocol/openid-connect/logout?id_token_hint={id_token}&post_logout_redirect_uri={post_logout_redirect_uri}"
            session.clear()
            logout_user()
            return redirect(logout_url)
        else:
            flash("Logout failed: Unable to retrieve id_token.")
            return redirect(url_for('index'))
    flash("Logout failed: Session does not contain authentication info.")
    return redirect(url_for('index'))

# Route handlers...
## Debug Route
@app.route('/crazy')
def crazy():
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo([
            'email', 'preferred_username', 'name', 'sub', 'given_name', 'family_name',
            'locale', 'zoneinfo', 'updated_at', 'email_verified', 'address', 'phone_number',
            'birthdate', 'nickname', 'profile', 'picture', 'website', 'gender', 'preferred_username',
            'given_name', 'family_name', 'middle_name', 'nickname', 'profile', 'picture', 'website',
            'gender', 'birthdate', 'zoneinfo', 'locale', 'updated_at', 'phone_number', 'phone_number_verified',
            'email', 'email_verified'
        ])
        if hasattr(current_user, 'id'):
            user_db_info = app.user_metadata_handler.get_user_by_id(current_user.id)
            if user_db_info:
                follow_mode = user_db_info['follow_mode']
                iframe_mode = user_db_info['iframe_mode']
                light_dark_mode = user_db_info['light_dark_mode']

                session_data = json.dumps(session, default=str)
                session_data_size_bytes = len(session_data.encode('utf-8'))

                app.logger.debug(f"OIDC User info: {user_info}")
                app.logger.debug(f"Current session data size: {session_data_size_bytes}")
                app.logger.debug(f"Current session data: {session_data}")
                app.logger.debug(f"User DB info: {user_db_info}")

                # Get all session information
                all_session_info = dict(session)  # Convert session to a dictionary
                # Get OIDC information
                oidc_info = {
                    'user_loggedin': oidc.user_loggedin,
                    'user_info': user_info,
                    'auth_token': session.get('oidc_auth_token')  # Example of accessing specific OIDC token
                }
                app.logger.debug(f"All session information: {all_session_info}")  # Log all session information
                app.logger.debug(f"OIDC information: {oidc_info}")  # Log OIDC information

                #Get info from current user
                
                current_user_data = current_user.__dict__  # Get all attributes of current_user

                app.logger.debug(f"All Current User info: {current_user_data}")

                return render_template('500.html', current_user=current_user, user_name=user_db_info['username'], user_email=user_db_info['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
            
            else:
                app.logger.debug("No user found in DB.")
        else:
            app.logger.debug("No user ID available in current_user.")

    session.clear()
    app.logger.debug("User is not logged in or session has expired.")
    return redirect(url_for('index'))

### Main Route
@app.route('/')
def index():
    # Define function to get a random logo
    def get_random_logo():
        logo_images = ['logo_on_lt_ukr.png', 'logo_on_ukr_lt.png']
        return random.choice(logo_images)
    
    statistics_id=Config.FAST_BI_STATISTICS_ID

    # Check if the user is authenticated
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username'])
        handle_user_login(user_info)  # Ensure this function is adapted to use UserConsoleMetadataHandler
        return redirect(url_for('homepage'))
    else:
        session.clear()
        logout_user()

        # Use your DELEVOPMENT_TEAM variable as per the current config
        if app.config['DELEVOPMENT_TEAM'] == True or app.config['DELEVOPMENT_TEAM'].lower() == 'true':
            logo_image_name = get_random_logo()
        else:
            logo_image_name = 'logo_transparent_grey.png'
        
        # Pass the logo image to the template
        return render_template('index.html', logo_image_name=logo_image_name, statistics_id=statistics_id)

### System Pages
# Production sites should use HTTPS.
# Health page route
@app.route("/health")
def health():
    response = jsonify({"message": "App up and running successfully. CORS is enabled!"})
    print(response.headers)  # Check if CORS headers are present
    return response

# User profile route
@app.route('/profile')
def user_profile():
    # Define function to get a random logo
    def get_random_logo():
        logo_images = ['logo_on_lt_ukr.png', 'logo_on_ukr_lt.png']
        return random.choice(logo_images)
    if oidc.user_loggedin:
        # Retrieve user details from the database using the metadata handler
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']
            return render_template('user_profile.html', current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # Use your DELEVOPMENT_TEAM variable as per the current config
            if app.config['DELEVOPMENT_TEAM'] == True or app.config['DELEVOPMENT_TEAM'].lower() == 'true':
                logo_image_name = get_random_logo()
            else:
                logo_image_name = 'logo_on_grey_purple.png'
            
            # Pass the logo image to the template
            return render_template('index.html', logo_image_name=logo_image_name, error="No user found.")
    return redirect(url_for('index'))

# Site configuration route
@app.route('/configuration')
def site_configuration():
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])
        # Retrieve user details from the database using the metadata handler
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Platform'
            role_based_groups = {'Admin'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                # Get the IDP_SSO_CONTROL_MODE value from the environment variable with a default of 'disabled'
                idp_sso_control_mode = app.config['IDP_SSO_CONTROL_MODE']
                idp_sso_control_mode_endpoint = app.config['IDP_SSO_LINK']
                # Check if SSO is enabled
                if idp_sso_control_mode == 'enabled':
                    # Redirect to the external login URL
                    return redirect(idp_sso_control_mode_endpoint)
                else:
                    return render_template('configuration.html', current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())

            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"

                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # User does not meet the access criteria, build dynamic allowed_groups_str
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

# Homepage route
@app.route('/homepage')
def homepage():
    # Check authentication first
    if not oidc.user_loggedin:
        return redirect(url_for('index'))
    # Only add cache for authenticated users
    #@app.cache.cached(timeout=7200, key_prefix=lambda: f'home_{current_user.id}_{session.get("_id")}_{current_user.follow_mode}_{current_user.iframe_mode}_{current_user.light_dark_mode}')
    # Add token expiry to cache key
    cache_key = (
        f'home_{current_user.id}_{session.get("_id")}_'
        f'{current_user.follow_mode}_{current_user.iframe_mode}_'
        f'{current_user.light_dark_mode}_{int(time.time() // 3600)}'
    )
    @app.cache.cached(timeout=600, key_prefix=lambda: cache_key)
    def get_homepage():
        welcome_message_js = None  # Default value for welcome_message
        now = int(time.time() * 1000)
        today = datetime.now()
        today_str = today.strftime("%Y-%m-%d")
        this_month_start = int(time.mktime(time.localtime()) * 1000) - (time.localtime().tm_mday - 1) * 86400 * 1000
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)

        if user_data:
            if user_data['first_login_time'] is None or user_data['first_login_time'] == user_data['last_login_time']:
                welcome_message = f"""Welcome, {user_data['username']}!
                Before you start using the fast.bi console, you will need to authenticate to all platform services.
                Simply open each service from the navigation tab and finalize the authentication.
                Make sure that your cross-site cookies are allowed in the browser.
                """
                welcome_message_js = welcome_message.replace('\n', '<br>')
                app.user_metadata_handler.update_login_time(current_user.id)

            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            env_variables = SourceConfig.get_environment_variables()
            monitoring_link = env_variables.get('monitoring_link')
            monitoring_basic_auth_user = env_variables.get('monitoring_basic_auth_user')
            monitoring_basic_auth_pass = env_variables.get('monitoring_basic_auth_pass')
            grafana_url = f"{monitoring_link}/api/prometheus/grafana/api/v1/alerts?includeInternalLabels=false"
            try:
                response = requests.get(grafana_url, headers={'accept': 'application/json'}, auth=(monitoring_basic_auth_user, monitoring_basic_auth_pass))
                if response.status_code == 200:
                    alert_data = response.json()
                    alerts = alert_data['data']['alerts']
                    alert_amount = len(alerts)
                    # Treat both 'Normal' and 'Normal (NoData)' as normal
                    normal_alerts = [alert for alert in alerts if alert['state'] == 'Normal' or alert['state'] == 'Normal (NoData)']
                    non_normal_alerts = [alert for alert in alerts if alert['state'] != 'Normal' and alert['state'] != 'Normal (NoData)']
                    error_alerts = [alert for alert in non_normal_alerts if alert['state'] == 'Error']
                    alerts_count_errors = len(error_alerts)
                    error_dates = [datetime.fromisoformat(alert['activeAt'][:-1]) for alert in error_alerts]
                    non_normal_dates = [datetime.fromisoformat(alert['activeAt'][:-1]) for alert in non_normal_alerts]
                    normal_dates = [datetime.fromisoformat(alert['activeAt'][:-1]) for alert in normal_alerts]

                    oldest_error_date = min(error_dates) if error_dates else None
                    latest_non_normal_date = max(non_normal_dates) if non_normal_dates else None
                    latest_normal_date = max(normal_dates) if normal_dates else None

                    auth_token = get_jwt_token(user_data['username'], user_data['email'])
                    return render_template('home.html', auth_token=auth_token, user_id=current_user.id, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, welcome_message=welcome_message_js, light_dark_mode=light_dark_mode, alert_amount=alert_amount, non_normal_alerts=len(non_normal_alerts), alerts_count_errors=alerts_count_errors, oldest_error_date=oldest_error_date, latest_non_normal_date=latest_non_normal_date, latest_normal_date=latest_normal_date, now=now, today=today_str, this_month_start=this_month_start,  **SourceConfig.get_environment_variables())
                else:
                    return render_template('500.html', error_message="Failed to fetch alert statistics from Grafana.")
            except requests.exceptions.RequestException as e:
                return render_template('500.html')
        return redirect(url_for('index'))
    return get_homepage()


### Platform Services

# DBT Project initialization route
@app.route('/dbt-init')
def dbt_init():
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Project_Initialization'
            role_based_groups = {'Admin', 'User'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has one of the required groups, proceed with rendering
                # Assuming you have airbyte_workspace_id and airbyte_connections variables available
                airbyte_workspace_id = get_airbyte_workspace_id()  # Function to get airbyte workspace ID
                airbyte_obj = json.dumps(get_airbyte_connections(airbyte_workspace_id))
                return render_template('dbt-project-initialization.html',
                                    current_user=current_user,
                                    user_name=user_data['username'],
                                    user_email=user_data['email'],
                                    airbyte_workspace_id=airbyte_workspace_id,
                                    airbyte_obj=airbyte_obj,
                                    follow_mode=follow_mode,
                                    iframe_mode=iframe_mode,
                                    light_dark_mode=light_dark_mode,
                                    **SourceConfig.get_environment_variables())
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"

                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

# DBT Project management route
@app.route('/dbt-management')
def dbt_management():
    # Check authentication first
    if not oidc.user_loggedin:
        return redirect(url_for('index'))

    # Only add cache for authenticated users
    @app.cache.cached(timeout=7200, key_prefix=lambda: f'dbt_mgmt_{current_user.id}_{session.get("_id")}_{current_user.follow_mode}_{current_user.iframe_mode}_{current_user.light_dark_mode}')
    def get_dbt_management():
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])
        
        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        
        if not user_data:
            return render_template('403.html', 
                                allowed_groups='User data not found', 
                                current_user=current_user, 
                                **SourceConfig.get_environment_variables())

        follow_mode = user_data['follow_mode']
        iframe_mode = user_data['iframe_mode']
        light_dark_mode = user_data['light_dark_mode']

        # Check if 'groups' is part of the response and act accordingly
        user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
        mandatory_group = '/Data Platform Services/Data_Project_Management'
        role_based_groups = {'Admin', 'User'}

        # Check for mandatory group membership and at least one of the role-based groups
        if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
            # The user has the mandatory group and one of the required role-based groups, proceed with rendering
            dbt_projects = dpm.get_dbt_projects()
            return render_template('dbt-project-management.html', 
                                dbt_projects=dbt_projects, 
                                current_user=current_user, 
                                user_name=user_data['username'], 
                                user_email=user_data['email'], 
                                follow_mode=follow_mode, 
                                iframe_mode=iframe_mode, 
                                light_dark_mode=light_dark_mode, 
                                **SourceConfig.get_environment_variables())
        else:
            # User does not meet the access criteria, build dynamic allowed_groups_str
            role_based_list = list(role_based_groups)
            allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1] if len(role_based_list) > 1 else role_based_list[0]
            allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"

            return render_template('403.html', 
                                allowed_groups=allowed_groups_str, 
                                current_user=current_user, 
                                user_name=user_data['username'], 
                                user_email=user_data['email'], 
                                follow_mode=follow_mode, 
                                iframe_mode=iframe_mode, 
                                light_dark_mode=light_dark_mode, 
                                **SourceConfig.get_environment_variables())

    return get_dbt_management()

# Platform Stats route
@app.route('/stats')
def stats():
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']
            user_groups = set(user_info.get('groups', []))
            mandatory_group = '/Data Platform Services/Data_Platform_Stats'
            role_based_groups = {'Admin', 'User', 'Viewer'}

            # Check global cache first
            stats_data = app.cache.get('global_stats')
            if stats_data is None:
                # If global cache is empty, fetch stats based on data warehouse type
                if app.config['FASTBI_PLATFORM_DWH'] == 'bigquery':
                    stats_data = get_bq_stats()
                elif app.config['FASTBI_PLATFORM_DWH'] == 'snowflake':
                    stats_data = get_sf_stats()
                elif app.config['FASTBI_PLATFORM_DWH'] == 'redshift':
                    stats_data = get_rd_stats()
                elif app.config['FASTBI_PLATFORM_DWH'] == 'fabric':
                    stats_data = get_ft_stats()

            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                return render_template('stats.html',
                    current_user=current_user,
                    user_name=user_data['username'],
                    user_email=user_data['email'],
                    follow_mode=follow_mode,
                    iframe_mode=iframe_mode,
                    light_dark_mode=light_dark_mode,
                    **stats_data,
                    **SourceConfig.get_environment_variables()
                )
            else:
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]
                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"
                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        return redirect(url_for('index'))

### Platform external Services

# Data Manipulation (ide console) route
@app.route('/data-manipulation')
def ide():
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Manipulation'
            role_based_groups = {'Admin', 'User'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                return render_template('ide_iframe.html', current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"

                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

# Data Manipulation (ide console) route
@app.route('/data-manipulation/admin-panel')
def ideahub():
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Manipulation'
            role_based_groups = {'Admin', 'User'}
            
            # Convert all group names to lowercase for case-insensitive comparison
            user_groups_lower = {group.lower() for group in user_groups}
            
            # Check for admin or user role (case-insensitive)
            if any(admin_group in user_groups_lower for admin_group in ['admin']):
                user_role = "admin#"
            elif any(user_group in user_groups_lower for user_group in ['user']):
                user_role = "home"
            else:
                user_role = "None"

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                return render_template('ide_iframe_admin_panel.html', user_role=user_role, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"

                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

# Data Replication route
@app.route('/data-replication')
def airbyte():
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Replication'
            role_based_groups = {'Admin', 'User'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                return render_template('airbyte_iframe.html', current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"

                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

# Data Orchestration route
@app.route('/data-orchestration')
def airflow():
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Orchestration'
            role_based_groups = {'Admin', 'User', 'Viewer'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                return render_template('airflow_iframe.html', current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"

                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

# Data Quality route
@app.route('/dq')
def data_quality_list_projects():
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Quality'
            role_based_groups = {'Admin', 'User', 'Viewer'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                return render_template('quality_iframe.html', current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"

                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

# Data Catalog route
@app.route('/dc')
def data_catalog_list_projects():
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Catalog'
            role_based_groups = {'Admin', 'User', 'Viewer'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                return render_template('catalog_iframe.html', current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"

                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

# Data Governance route
@app.route('/data-governance')
def datahub():
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Governance'
            role_based_groups = {'Admin', 'User', 'Viewer'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                return render_template('datahub_iframe.html', current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"

                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

# Data Platform CICD Workflows route
@app.route('/data-platform-workflows')
def argo_workflows():
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Platform'
            role_based_groups = {'Admin', 'User', 'Viewer'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                return render_template('cicd_workflow_iframe.html', current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"

                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

### WebSite_System_Functions
@app.route('/update_follow_mode', methods=['POST'])
def update_follow_mode():
    if oidc.user_loggedin:
        new_follow_mode = request.form.get('follow_mode')

        # Get the current logged-in user's ID from current_user
        if hasattr(current_user, 'id'):
            user_id = current_user.id
            user = app.user_metadata_handler.get_user_by_id(user_id)
            if user:
                app.user_metadata_handler.update_user(user_id, follow_mode=new_follow_mode)
                return jsonify({'message': 'Follow mode updated successfully'})
            else:
                return jsonify({'error': 'User not found'}), 404
        else:
            return jsonify({'error': 'Current user ID not available'}), 400
    else:
        return jsonify({'error': 'User is not logged in'}), 401

@app.route('/update_iframe_mode', methods=['POST'])
def update_iframe_mode():
    if oidc.user_loggedin:
        new_iframe_mode = request.form.get('iframe_mode')

        # Get the current logged-in user's ID from current_user
        if hasattr(current_user, 'id'):
            user_id = current_user.id
            user = app.user_metadata_handler.get_user_by_id(user_id)
            if user:
                app.user_metadata_handler.update_user(user_id, iframe_mode=new_iframe_mode)
                return jsonify({'message': 'Iframe mode updated successfully'})
            else:
                return jsonify({'error': 'User not found'}), 404
        else:
            return jsonify({'error': 'Current user ID not available'}), 400
    else:
        return jsonify({'error': 'User is not logged in'}), 401

@app.route('/update_light_dark_mode', methods=['POST'])
def update_light_dark_mode():
    if oidc.user_loggedin:
        new_light_dark_mode = request.form.get('light_dark_mode')

        # Assuming current_user is correctly populated by your OIDC setup
        if not hasattr(current_user, 'id'):
            return jsonify({'error': 'Current user ID not available'}), 400

        user_id = current_user.id  # Use the logged-in user's ID directly
        user = app.user_metadata_handler.get_user_by_id(user_id)
        if user:
            app.user_metadata_handler.update_user(user_id, light_dark_mode=new_light_dark_mode)
            return jsonify({'message': 'Light dark mode updated successfully'})
        else:
            return jsonify({'error': 'User not found'}), 404
    else:
        return jsonify({'error': 'User is not logged in'}), 401

# Deprecated
# @app.route('/mobile-app')
# def mobile():
#     if oidc.user_loggedin:
#         user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
#         if user_data:
#             return render_template('fast-bi-homepage.html', current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], **SourceConfig.get_environment_variables())
#         else:
#             return redirect(url_for('index'), error="User not found.")
#     else:
#         return redirect(url_for('index'))

@app.route('/data-catalog')
def data_catalog():
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Catalog'
            role_based_groups = {'Admin', 'User', 'Viewer'}
            action_toggle_status = {'Admin', 'User'}
            action_delete = {'Admin'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                action_toggle_status_enable = None
                action_delete_enable = None
                url = f"{app.config['DC_DQ_ENDPOINT_URL']}/data-catalog"
                headers = {'Authorization': app.config['BEARER_TOKEN']}
                response = requests.get(url, headers=headers)
                if response.status_code == 500:
                    return render_template('500.html', error_message="Failed to fetch data catalog projects. Please try again later.", current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
                projects = response.json()
                if not user_groups.isdisjoint(action_toggle_status):
                    action_toggle_status_enable=True
                if not user_groups.isdisjoint(action_delete):
                    action_delete_enable=True
                return render_template('data_catalog.html', projects=projects, action_toggle_status_enable=action_toggle_status_enable, action_delete_enable=action_delete_enable, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"
                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

@app.route('/data-catalog/live/<path:partial_url>')
def data_catalog_live(partial_url):
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Catalog'
            role_based_groups = {'Admin', 'User', 'Viewer'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                projects_url = f"{app.config['DC_DQ_ENDPOINT_URL']}/data-catalog"
                headers = {'Authorization': app.config['BEARER_TOKEN']}
                response = requests.get(projects_url, headers=headers)
                if response.status_code == 500:
                    return render_template('500.html', error_message="Failed to fetch data catalog projects. Please try again later.", current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
                projects = response.json()
                url = 'https://' + unquote(partial_url)

                # Find the project by endpoint link
                project = next((p for p in projects if p.get('dbt_project_endpoint_link') == url), None)
                if project and not project.get('online_status', False):
                    # Service is offline
                    return render_template('503_dc.html', current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
                # If project is online or not found, show iframe
                return render_template('data_catalog_iframe.html', data_catalog_endpoint=url, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"
                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

@app.route('/data-catalog/refresh/<int:id>')
def data_catalog_refresh_project(id):
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Catalog'
            role_based_groups = {'Admin', 'User'}
            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                url = f"{app.config['DC_DQ_ENDPOINT_URL']}/data-catalog/{id}/refresh-project"
                headers = {'Authorization': app.config['BEARER_TOKEN']}
                data = {'updated_at': datetime.utcnow().isoformat() + 'Z'}
                requests.patch(url, headers=headers, json=data)
                return redirect(url_for('data_catalog'))
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"
                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

@app.route('/data-catalog/toggle_status/<string:project_name>')
def data_catalog_toggle_status(project_name):
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Catalog'
            role_based_groups = {'Admin', 'User'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                # Fetch current status first
                project_url = f"{app.config['DC_DQ_ENDPOINT_URL']}/data-catalog/{project_name}"
                headers = {'Authorization': app.config['BEARER_TOKEN']}
                project = requests.get(project_url, headers=headers).json()
                project_id = project['id']
                new_status = not project['online_status']
                # Update the status
                url = f"{app.config['DC_DQ_ENDPOINT_URL']}/data-catalog/{project_id}/online-status"
                # "updated_at": "2024-06-04T15:47:40.364Z"
                data = {'online_status': new_status, 'updated_at': datetime.utcnow().isoformat() + 'Z'}
                requests.patch(url, headers=headers, json=data)
                return redirect(url_for('data_catalog'))
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"
                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

@app.route('/data-catalog/logstream/<int:id>', methods=['GET'])
def data_catalog_project_logstream(id):
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Catalog'
            role_based_groups = {'Admin', 'User', 'Viewer'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                url = f"{app.config['DC_DQ_ENDPOINT_URL']}/data-catalog/{id}/logs"
                headers = {'Authorization': app.config['BEARER_TOKEN']}
                try:
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    logs_data = response.json()
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching logs from external service: {e}")
                    return {}
                logs = logs_data.get("logs") or {}
                if not logs:
                    logs = {'No logs found. Please check the project ID.'}
                return logs
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"
                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

@app.route('/data-catalog/delete/<int:id>')
def data_catalog_delete_project(id):
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Catalog'
            role_based_groups = {'Admin'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                url = f"{app.config['DC_DQ_ENDPOINT_URL']}/data-catalog/{id}"
                headers = {'Authorization': app.config['BEARER_TOKEN']}
                requests.delete(url, headers=headers)
                return redirect(url_for('data_catalog'))
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"
                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

#####

@app.route('/data-quality')
def data_quality():
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Quality'
            role_based_groups = {'Admin', 'User', 'Viewer'}
            action_toggle_status = {'Admin', 'User'}
            action_delete = {'Admin'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                action_toggle_status_enable = None
                action_delete_enable = None
                url = f"{app.config['DC_DQ_ENDPOINT_URL']}/data-quality"
                headers = {'Authorization': app.config['BEARER_TOKEN']}
                response = requests.get(url, headers=headers)
                if response.status_code == 500:
                    return render_template('500.html', error_message="Failed to fetch data quality projects. Please try again later.", current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
                projects = response.json()
                if not user_groups.isdisjoint(action_toggle_status):
                    action_toggle_status_enable=True
                if not user_groups.isdisjoint(action_delete):
                    action_delete_enable=True
                return render_template('data_quality.html', projects=projects, action_toggle_status_enable=action_toggle_status_enable, action_delete_enable=action_delete_enable, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"
                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

@app.route('/data-quality/live/<path:partial_url>')
def data_quality_live(partial_url):
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Quality'
            role_based_groups = {'Admin', 'User', 'Viewer'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                projects_url = f"{app.config['DC_DQ_ENDPOINT_URL']}/data-quality"
                headers = {'Authorization': app.config['BEARER_TOKEN']}
                response = requests.get(projects_url, headers=headers)
                if response.status_code == 500:
                    return render_template('500.html', error_message="Failed to fetch data quality projects. Please try again later.", current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
                projects = response.json()
                url = 'https://' + unquote(partial_url)

                # Find the project by endpoint link
                project = next((p for p in projects if p.get('dbt_project_endpoint_link') == url), None)
                if project and not project.get('online_status', False):
                    # Service is offline
                    return render_template('503_dq.html', current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
                # If project is online or not found, show iframe
                return render_template('data_quality_iframe.html', data_quality_endpoint=url, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"
                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

@app.route('/data-quality/refresh/<int:id>')
def data_quality_refresh_project(id):
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Quality'
            role_based_groups = {'Admin', 'User'}
            print(user_groups)
            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                url = f"{app.config['DC_DQ_ENDPOINT_URL']}/data-quality/{id}/refresh-project"
                headers = {'Authorization': app.config['BEARER_TOKEN']}
                data = {'updated_at': datetime.utcnow().isoformat() + 'Z'}
                requests.patch(url, headers=headers, json=data)
                return redirect(url_for('data_quality'))
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"
                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

@app.route('/data-quality/toggle_status/<string:project_name>')
def data_quality_toggle_status(project_name):
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Quality'
            role_based_groups = {'Admin', 'User'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                # Fetch current status first
                project_url = f"{app.config['DC_DQ_ENDPOINT_URL']}/data-quality/{project_name}"
                headers = {'Authorization': app.config['BEARER_TOKEN']}
                project = requests.get(project_url, headers=headers).json()
                project_id = project['id']
                new_status = not project['online_status']
                # Update the status
                url = f"{app.config['DC_DQ_ENDPOINT_URL']}/data-quality/{project_id}/online-status"
                # "updated_at": "2024-06-04T15:47:40.364Z"
                data = {'online_status': new_status, 'updated_at': datetime.utcnow().isoformat() + 'Z'}
                requests.patch(url, headers=headers, json=data)
                return redirect(url_for('data_quality'))
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"
                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

@app.route('/data-quality/logstream/<int:id>', methods=['GET'])
def data_quality_project_logstream(id):
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Quality'
            role_based_groups = {'Admin', 'User', 'Viewer'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                url = f"{app.config['DC_DQ_ENDPOINT_URL']}/data-quality/{id}/logs"
                headers = {'Authorization': app.config['BEARER_TOKEN']}
                try:
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    logs_data = response.json()
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching logs from external service: {e}")
                    return {}
                logs = logs_data.get("logs") or {}
                if not logs:
                    logs = {'No logs found. Please check the project ID.'}
                return logs
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"
                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

@app.route('/data-quality/delete/<int:id>')
def data_quality_delete_project(id):
    if oidc.user_loggedin:
        user_info = oidc.user_getinfo(['email', 'preferred_username', 'groups'])

        # Get User Info Data from DB
        user_data = app.user_metadata_handler.get_user_by_id(current_user.id)
        if user_data:
            follow_mode = user_data['follow_mode']
            iframe_mode = user_data['iframe_mode']
            light_dark_mode = user_data['light_dark_mode']

            # Check if 'groups' is part of the response and act accordingly
            user_groups = set(user_info.get('groups', []))  # Convert to set for easier manipulation
            mandatory_group = '/Data Platform Services/Data_Quality'
            role_based_groups = {'Admin'}

            # Check for mandatory group membership and at least one of the role-based groups
            if mandatory_group in user_groups and not user_groups.isdisjoint(role_based_groups):
                # The user has the mandatory group and one of the required role-based groups, proceed with rendering
                url = f"{app.config['DC_DQ_ENDPOINT_URL']}/data-quality/{id}"
                headers = {'Authorization': app.config['BEARER_TOKEN']}
                requests.delete(url, headers=headers)
                return redirect(url_for('data_quality'))
            else:
                # User does not meet the access criteria, build dynamic allowed_groups_str
                role_based_list = list(role_based_groups)
                if len(role_based_list) > 1:
                    allowed_roles_str = ', '.join(role_based_list[:-1]) + ', or ' + role_based_list[-1]
                else:
                    allowed_roles_str = role_based_list[0]

                allowed_groups_str = f"{mandatory_group}, and {allowed_roles_str}"
                return render_template('403.html', allowed_groups=allowed_groups_str, current_user=current_user, user_name=user_data['username'], user_email=user_data['email'], follow_mode=follow_mode, iframe_mode=iframe_mode, light_dark_mode=light_dark_mode, **SourceConfig.get_environment_variables())
        else:
            # No user data found, handle appropriately
            return render_template('403.html', allowed_groups='User data not found', current_user=current_user, **SourceConfig.get_environment_variables())
    else:
        # User not logged in
        return redirect(url_for('index'))

@app.route('/dbt-management/cache/clear', methods=['POST'])
def clear_cache():
    # Check authentication only
    if not oidc.user_loggedin:
        return jsonify({"error": "Authentication required"}), 401
    
    try:
        # Clear cache for the current user only
        user_id = current_user.id
        session_id = session.get("_id", "")
        follow_mode = current_user.follow_mode
        iframe_mode = current_user.iframe_mode
        light_dark_mode = current_user.light_dark_mode
        
        # Build the cache key pattern for this user
        cache_key_prefix = f'dbt_mgmt_{user_id}_{session_id}_{follow_mode}_{iframe_mode}_{light_dark_mode}'
        
        # Clear the specific cache key
        # This approach works for both Redis and other cache backends
        app.cache.delete(cache_key_prefix)
        
        # If using Redis directly, we can try this pattern-based approach
        if hasattr(app.cache, 'cache') and hasattr(app.cache.cache, 'delete_pattern'):
            # Some Redis cache implementations support pattern deletion
            app.cache.cache.delete_pattern(f"{cache_key_prefix}*")
            
        # Alternatively, manually construct and delete the actual key
        actual_cache_key = f"dbt_mgmt_{user_id}_{session_id}_{follow_mode}_{iframe_mode}_{light_dark_mode}"
        app.cache.delete(actual_cache_key)
            
        # For simple cache backends, mark it directly for removal
        if hasattr(app.cache, 'get') and hasattr(app.cache, 'set'):
            # Get current value and re-cache with 1 second timeout to effectively delete
            app.cache.set(actual_cache_key, None, timeout=1)
        
        return jsonify({
            "success": True,
            "message": f"Cache cleared successfully for user: {user_id}",
            "keys_cleared": 1
        })
    
    except Exception as e:
        app.logger.error(f"Error clearing cache: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.context_processor
def inject_logo_path():
    instance_path = os.path.abspath('instance')
    logo_path = os.path.join(instance_path, 'custom_logo.png')
    if os.path.exists(logo_path):
        # Use a cache buster timestamp to ensure updates are seen immediately
        # In a real scenario, we might check file modification time, but time.time() is sufficient for this purpose if called once per request or handled in JS.
        # For context processor, we'll just provide the base path and let JS handle cache busting or rely on browser cache behavior.
        # Actually, to avoid flicker, we should point directly to the custom logo endpoint.
        return dict(logo_path='/ui-config/custom_logo')
    else:
        return dict(logo_path='/images/logo_transparent.png')

# Custom Logo Feature
@app.route('/ui-config/upload_logo', methods=['POST'])
def upload_logo():
    if not oidc.user_loggedin:
        return jsonify({'error': 'Unauthorized'}), 401
    
    user_info = oidc.user_getinfo(['groups'])
    user_groups = set(user_info.get('groups', []))
    user_groups_lower = {group.lower() for group in user_groups}
    
    # Check for Admin role (case-insensitive)
    if not any(admin_group in user_groups_lower for admin_group in ['admin', 'consoleadmin']):
         return jsonify({'error': 'Forbidden: Admin access required'}), 403

    if 'logo' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['logo']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
        
    if file:
        filename = "custom_logo.png" # Force filename to ensure consistency
        # Ensure instance directory exists
        instance_path = os.path.abspath('instance')
        if not os.path.exists(instance_path):
            os.makedirs(instance_path)
            
        file.save(os.path.join(instance_path, filename))
        return jsonify({'message': 'Logo uploaded successfully'}), 200

@app.route('/ui-config/custom_logo')
def custom_logo():
    instance_path = os.path.abspath('instance')
    return send_from_directory(instance_path, 'custom_logo.png')

@app.route('/ui-config/logo_status')
def logo_status():
    instance_path = os.path.abspath('instance')
    logo_path = os.path.join(instance_path, 'custom_logo.png')
    if os.path.exists(logo_path):
        return jsonify({'has_custom_logo': True, 'url': '/ui-config/custom_logo'}), 200
    else:
        return jsonify({'has_custom_logo': False}), 200

if __name__ == "__main__":
    # Determine the environment and configure Flask accordingly
    if  app.config['FLASK_ENV'] == "production":
        app.run(host="0.0.0.0", port=8080, debug=False)
    else:
        app.run(host="0.0.0.0", port=8080, debug=True)
