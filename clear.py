import os
import logging
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

# Configure logging 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/notion_task_cleaner_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('notion_task_cleaner')

# Load environment variables from .env file
load_dotenv()

# Notion API credentials
NOTION_API_TOKEN = os.getenv("NOTION_API_TOKEN")
DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

# Log configuration details
logger.info(f"Starting Notion Task Cleaner")
logger.info(f"Database ID: {DATABASE_ID}")
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

def get_templated_tasks_with_dates():
    """
    Fetch templated tasks that have a date (not empty)
    """
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    
    # Query for tasks with "Task Type" = "Templated task" and non-empty Date
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
                        "is_not_empty": True
                    }
                }
            ]
        }
    }
    
    logger.info(f"Fetching templated tasks with dates from database: {DATABASE_ID}")
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        data = response.json()
        
        if response.status_code != 200:
            logger.error(f"Error fetching tasks: {data}")
            return []
        
        results = data.get("results", [])
        logger.info(f"Successfully fetched {len(results)} templated tasks with dates")
        return results
    except Exception as e:
        logger.exception(f"Exception when fetching tasks: {str(e)}")
        return []

def delete_task(task_id, task_name):
    """
    Delete a task from Notion by its ID
    """
    url = f"https://api.notion.com/v1/pages/{task_id}"
    
    logger.info(f"Deleting task: {task_name} (ID: {task_id})")
    
    try:
        # Notion API doesn't actually delete pages, it archives them
        response = requests.patch(url, headers=headers, json={"archived": True})
        
        if response.status_code == 200:
            logger.info(f"Successfully deleted task: {task_name}")
            return True
        else:
            logger.error(f"Error deleting task: {task_name}")
            logger.error(f"API response: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        logger.exception(f"Exception when deleting task '{task_name}': {str(e)}")
        return False

def main():
    """
    Main entry point of the script
    """
    try:
        logger.info("Starting Notion Task Cleaner")
        
        # Check for required environment variables
        if not NOTION_API_TOKEN:
            logger.error("Missing NOTION_API_TOKEN environment variable. Cannot continue.")
            return 0
            
        if not DATABASE_ID:
            logger.error("Missing NOTION_DATABASE_ID environment variable. Cannot continue.")
            return 0
        
        # Get all templated tasks with dates
        tasks = get_templated_tasks_with_dates()
        if not tasks:
            logger.info("No templated tasks with dates found. Nothing to delete.")
            return 0
        
        logger.info(f"Found {len(tasks)} templated tasks with dates to delete")
        
        # Confirm with user before deletion
        print(f"About to delete {len(tasks)} templated tasks with dates.")
        confirmation = input("Are you sure you want to continue? (yes/no): ")
        
        if confirmation.lower() not in ["yes", "y"]:
            logger.info("Deletion cancelled by user")
            print("Deletion cancelled.")
            return 0
        
        # Delete tasks
        successful_deletions = 0
        for task in tasks:
            # Extract task details
            task_id = task.get("id")
            task_name = "Unnamed Task"
            
            try:
                task_name = task.get("properties", {}).get("Task", {}).get("title", [{}])[0].get("text", {}).get("content", "Unnamed Task")
            except (IndexError, KeyError):
                pass
            
            # Delete the task
            if delete_task(task_id, task_name):
                successful_deletions += 1
        
        logger.info(f"Task deletion complete. Successfully deleted {successful_deletions} of {len(tasks)} tasks.")
        print(f"Successfully deleted {successful_deletions} of {len(tasks)} tasks.")
        
        return successful_deletions
        
    except Exception as e:
        logger.exception(f"Unexpected error in main function: {str(e)}")
        return 0

if __name__ == "__main__":
    try:
        deleted_count = main()
        logger.info(f"Script completed. Deleted {deleted_count} tasks.")
    except Exception as e:
        logger.exception(f"Unhandled exception in script: {str(e)}")
        logger.error("Script terminated due to error.")