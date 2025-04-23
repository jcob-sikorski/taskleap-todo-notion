import os
import logging
import json
import math
from datetime import datetime, date, timedelta
from collections import defaultdict
import time
import requests
from dotenv import load_dotenv

# Configure logging 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/notion_task_duplicator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('notion_task_duplicator')

# Load environment variables from .env file
load_dotenv()

# Notion API credentials
NOTION_API_TOKEN = os.getenv("NOTION_API_TOKEN")
DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

# Configuration
MAX_TASKS_PER_DAY = 3  # Maximum number of templated tasks allowed per day

# Log configuration details
logger.info(f"Starting Notion Task Duplicator")
logger.info(f"Database ID: {DATABASE_ID}")
logger.info(f"Maximum tasks per day: {MAX_TASKS_PER_DAY}")
if not NOTION_API_TOKEN:
    logger.error("NOTION_API_TOKEN not found in environment variables")
if not DATABASE_ID:
    logger.error("NOTION_DATABASE_ID not found in environment variables")

# Notion API headers
headers = {
    "Authorization": f"Bearer {NOTION_API_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

def get_database_schema():
    """
    Get the schema of the database to understand property types
    """
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.error(f"Error fetching database schema: {response.status_code}")
            logger.error(response.text)
            return {}
            
        data = response.json()
        properties = data.get('properties', {})
        
        # Create a mapping of property name to type
        schema = {}
        for name, details in properties.items():
            prop_type = details.get('type')
            schema[name] = prop_type
            
        logger.info(f"Database schema retrieved with {len(schema)} properties")
        logger.debug(f"Schema: {json.dumps(schema)}")
        return schema
    except Exception as e:
        logger.exception(f"Error getting database schema: {str(e)}")
        return {}

def get_templated_tasks():
    """
    Fetch templated tasks with empty dates from the Notion database
    """
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    
    # Query for tasks with "Task Type" = "Templated task" and empty Date
    payload = {
        "filter": {
            "and": [
                {
                    "property": "Task Type",
                    "select": {
                        "equals": "Templated task"
                    }
                },
                {
                    "property": "Date",
                    "date": {
                        "is_empty": True
                    }
                }
            ]
        }
    }
    
    logger.info(f"Fetching templated tasks from database: {DATABASE_ID}")
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        data = response.json()
        
        if response.status_code != 200:
            logger.error(f"Error fetching tasks: {data}")
            return []
        
        results = data.get("results", [])
        logger.info(f"Successfully fetched {len(results)} templated tasks")
        return results
    except Exception as e:
        logger.exception(f"Exception when fetching tasks: {str(e)}")
        return []

def get_existing_tasks(start_date, end_date):
    """
    Fetch existing tasks in the date range to avoid exceeding daily limits
    """
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    
    # Format dates for the API
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    # Query for tasks with dates in the specified range
    payload = {
        "filter": {
            "and": [
                {
                    "property": "Date",
                    "date": {
                        "on_or_after": start_date_str
                    }
                },
                {
                    "property": "Date",
                    "date": {
                        "on_or_before": end_date_str
                    }
                }
            ]
        }
    }
    
    logger.info(f"Fetching existing tasks from {start_date_str} to {end_date_str}")
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        data = response.json()
        
        if response.status_code != 200:
            logger.error(f"Error fetching existing tasks: {data}")
            return {}
        
        results = data.get("results", [])
        
        # Count tasks by date
        tasks_by_date = defaultdict(int)
        for task in results:
            try:
                date_prop = task.get("properties", {}).get("Date", {}).get("date", {})
                if date_prop and date_prop.get("start"):
                    task_date = date_prop.get("start").split("T")[0]  # Extract just the date part
                    tasks_by_date[task_date] += 1
            except Exception as e:
                logger.warning(f"Error processing task date: {str(e)}")
        
        logger.info(f"Found {len(results)} existing tasks in date range")
        logger.info(f"Tasks by date: {dict(tasks_by_date)}")
        return tasks_by_date
        
    except Exception as e:
        logger.exception(f"Exception when fetching existing tasks: {str(e)}")
        return {}

def extract_task_properties(task, schema):
    """
    Extract relevant properties from a Notion task
    """
    try:
        task_id = task.get("id", "unknown_id")
        logger.info(f"Extracting properties for task: {task_id}")
        
        properties = task.get("properties", {})
        
        # Extract name (title)
        name = ""
        name_data = properties.get("Task", {}).get("title", [])
        if name_data:
            name = name_data[0].get("text", {}).get("content", "")
        logger.debug(f"Extracted name: {name}")
        
        # Extract Regularity (days) (number)
        regularity_days = properties.get("Regularity (days)", {}).get("number", 1) or 1
        logger.debug(f"Extracted regularity_days: {regularity_days}")
        
        # Create a property map to store all properties with correct types
        property_map = {
            "name": name,
            "regularity_days": regularity_days
        }
        
        # Extract all properties based on their type in the schema
        for prop_name, prop_details in properties.items():
            prop_type = schema.get(prop_name, "unknown")
            
            # Skip properties we've already processed
            if prop_name in ["Task", "Regularity (days)"]:
                continue
                
            # Extract based on property type
            if prop_type == "checkbox":
                property_map[f"{prop_name}_checkbox"] = prop_details.get("checkbox", False)
            elif prop_type == "rich_text":
                property_map[f"{prop_name}_rich_text"] = prop_details.get("rich_text", [])
            elif prop_type == "url":
                property_map[f"{prop_name}_url"] = prop_details.get("url")
            elif prop_type == "select":
                select_data = prop_details.get("select", {})
                if select_data:
                    property_map[f"{prop_name}_select"] = select_data.get("name")
            elif prop_type == "date":
                date_data = prop_details.get("date", {})
                if date_data:
                    property_map[f"{prop_name}_date"] = date_data.get("start")
        
        logger.info(f"Successfully extracted properties for task: {name} (regularity: {regularity_days})")
        return property_map
        
    except Exception as e:
        logger.exception(f"Error extracting task properties: {str(e)}")
        return {
            "name": "Error extracting task",
            "regularity_days": 1
        }

def create_task(properties, due_date, schema):
    """
    Create a new task in Notion based on template properties and due date
    """
    url = "https://api.notion.com/v1/pages"
    
    task_name = properties.get("name", "Unnamed task")
    logger.info(f"Creating new task: '{task_name}' for date: {due_date.strftime('%Y-%m-%d')}")
    
    # Prepare task properties
    new_properties = {
        "Task": {
            "title": [
                {
                    "text": {
                        "content": task_name
                    }
                }
            ]
        },
        "Task Type": {
            "select": {
                "name": "Templated task"
            }
        },
        "Date": {
            "date": {
                "start": due_date.strftime("%Y-%m-%d")
            }
        }
    }
    
    # Add all properties based on their type in the schema
    for prop_name, prop_type in schema.items():
        # Skip properties we've already set
        if prop_name in ["Task", "Task Type", "Date"]:
            continue
            
        # Set property based on its type
        if prop_type == "checkbox":
            new_properties[prop_name] = {
                "checkbox": properties.get(f"{prop_name}_checkbox", False)
            }
        elif prop_type == "rich_text":
            rich_text_value = properties.get(f"{prop_name}_rich_text", [])
            new_properties[prop_name] = {"rich_text": rich_text_value}
        elif prop_type == "url":
            url_value = properties.get(f"{prop_name}_url")
            # Important fix: For URL fields, use null instead of empty string
            new_properties[prop_name] = {"url": url_value if url_value else None}
        elif prop_type == "select":
            select_value = properties.get(f"{prop_name}_select")
            if select_value:
                new_properties[prop_name] = {"select": {"name": select_value}}
        elif prop_type == "date":
            date_value = properties.get(f"{prop_name}_date")
            if date_value:
                new_properties[prop_name] = {"date": {"start": date_value}}
    
    # Special case: Always set Done to false for new tasks
    if "Done" in schema and schema["Done"] == "checkbox":
        new_properties["Done"] = {"checkbox": False}
    
    task_data = {
        "parent": {"database_id": DATABASE_ID},
        "properties": new_properties
    }
    
    # Debug log the full properties being sent
    logger.debug(f"Full properties for task creation: {json.dumps(new_properties)}")
    
    try:
        response = requests.post(url, headers=headers, json=task_data)
        data = response.json()
        
        if response.status_code not in [200, 201]:
            logger.error(f"Error creating task: {task_name}")
            logger.error(f"API response: {data}")
            return None
        
        new_task_id = data.get("id", "unknown")
        logger.info(f"Successfully created task: '{task_name}' (ID: {new_task_id}, date: {due_date.strftime('%Y-%m-%d')})")
        return data
        
    except Exception as e:
        logger.exception(f"Exception when creating task '{task_name}': {str(e)}")
        return None

def schedule_tasks():
    """
    Main function to schedule tasks based on templates
    """
    # Get current date and month details
    today = date.today()
    current_month = today.month
    current_year = today.year
    last_day_of_month = datetime(current_year, current_month, 1).replace(day=28) + timedelta(days=4)
    end_of_month = (last_day_of_month.replace(day=1) - timedelta(days=1)).date()
    
    logger.info(f"Scheduling tasks from {today} to {end_of_month}")
    
    # First get the database schema to know property types
    schema = get_database_schema()
    if not schema:
        logger.error("Failed to retrieve database schema. Cannot continue.")
        return []
    
    # Get existing tasks to check daily limits
    existing_tasks_by_date = get_existing_tasks(today, end_of_month)
    
    # Track tasks we'll create to update our daily counts
    new_tasks_by_date = defaultdict(int)
    
    # Get all template tasks
    template_tasks = get_templated_tasks()
    if not template_tasks:
        logger.warning("No templated tasks found. Nothing to schedule.")
        return []
    
    logger.info(f"Found {len(template_tasks)} templated tasks")
    
    created_tasks = []
    
    # Process each template task
    for index, task in enumerate(template_tasks):
        logger.info(f"Processing template {index+1}/{len(template_tasks)}")
        
        # Extract task properties
        task_properties = extract_task_properties(task, schema)
        task_name = task_properties.get("name", "Unnamed task")
        
        # Get regularity in days
        regularity = task_properties.get("regularity_days", 1)
        if not regularity:
            logger.info(f"Skipping task without regularity: {task_name}")
            continue
        
        logger.info(f"Processing template: {task_name} (every {regularity} days)")
        
        # Start from today and create tasks until end of month
        current_date = today
        
        # Loop to create tasks with proper spacing based on regularity
        task_count = 0
        while current_date <= end_of_month:
            # Get date string for checking counts
            date_str = current_date.strftime("%Y-%m-%d")
            
            # Check if we've hit the daily limit
            total_tasks_for_day = existing_tasks_by_date.get(date_str, 0) + new_tasks_by_date[date_str]
            
            if total_tasks_for_day >= MAX_TASKS_PER_DAY:
                logger.info(f"Skipping task for {date_str} - daily limit of {MAX_TASKS_PER_DAY} reached")
                # Move to next date based on regularity
                current_date = current_date + timedelta(days=regularity)
                continue
            
            # Create the task
            logger.info(f"Creating task {task_count+1} for {task_name} on {current_date} (day has {total_tasks_for_day}/{MAX_TASKS_PER_DAY} tasks)")
            result = create_task(task_properties, current_date, schema)
            
            if result:
                created_tasks.append(result)
                new_tasks_by_date[date_str] += 1
                task_count += 1
            
            # IMPORTANT: Increment the date by the regularity value BEFORE next iteration
            current_date = current_date + timedelta(days=regularity)
            logger.info(f"Next task date for {task_name} would be {current_date}")
            
        # Add small delay between processing templates to avoid rate limiting
        if index < len(template_tasks) - 1:
            time.sleep(1)
    
    # Summary logging
    logger.info(f"Completed scheduling. {len(created_tasks)} tasks created for {today.month}/{today.year}")
    logger.info(f"Tasks per day after scheduling: {dict({**existing_tasks_by_date, **new_tasks_by_date})}")
    return created_tasks

def main():
    """
    Main entry point of the script
    """
    try:
        logger.info("Starting Notion Task Scheduler")
        
        # Check for required environment variables
        if not NOTION_API_TOKEN:
            logger.error("Missing NOTION_API_TOKEN environment variable. Cannot continue.")
            return []
            
        if not DATABASE_ID:
            logger.error("Missing NOTION_DATABASE_ID environment variable. Cannot continue.")
            return []
        
        # Run the task scheduling
        created_tasks = schedule_tasks()
        
        # Log success
        logger.info(f"Script completed successfully. Created {len(created_tasks)} scheduled tasks.")
        return created_tasks
        
    except Exception as e:
        logger.exception(f"Unexpected error in main function: {str(e)}")
        return []

if __name__ == "__main__":
    try:
        result = main()
        logger.info(f"Script completed. Created {len(result)} tasks.")
    except Exception as e:
        logger.exception(f"Unhandled exception in script: {str(e)}")
        logger.error("Script terminated due to error.")