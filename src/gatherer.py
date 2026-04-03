import os, glob, json, base64, time
import logging
from db_handler import DatabaseHandler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("usage-monitor")

def scan_directory(scratch_dir):
    '''
    Scans the scratch_root directory to aquire all jobs for their parameters. 
    Yields a dictionary with the following keys: job_id, session_id, instrument, product_type, job_status, user_email, user_roles.,
    for each job found in the scratch directory. The function is designed to be used in a loop, where it will continuously 
    scan the directory every N seconds to catch new jobs that have been added since the last scan. Scanned jobs
    then are going to be saved in the database and occasionally updated if there are any changes in the parameters (e.g. job status).
    '''
    #Scanning for parameters
    paths_for_analysis_parameters = os.path.join(scratch_dir, "scratch_sid_*/analysis_parameters.json")
    analysis_files = glob.glob(paths_for_analysis_parameters)
    
    for job_analysis_path in analysis_files:
        #Preventing situation when job_monitor is not created yet by dispatcher, but analysis_parameters is already there.
        folder_path = os.path.dirname(job_analysis_path)
        job_monitor_path = os.path.join(folder_path, "job_monitor.json")

        if not os.path.exists(job_monitor_path):
            continue

        try:
            with open(job_analysis_path, 'r') as f:
                data = json.load(f)
                instrument = data.get("instrument", "Unknown instrument") 
                product_type = data.get("product_type", "Unknown product type")
                jwt_token = data.get("token", "Anonymous") #When user is not authenticated, there will be no token field
            with open(job_monitor_path, 'r') as f:
                data = json.load(f)
                job_id = data.get("job_id", "Unknown job ID")
                session_id = data.get("session_id", "Unknown session ID")
                job_status = data.get("status", "Unknown job status")

        except json.JSONDecodeError:
            # The file is currently being written by the dispatcher
            continue
        except FileNotFoundError:
            # The cleaner sidecar literally just deleted this folder
            continue

        #Converting token from base64
        if jwt_token != "Anonymous":
            try:
                payload = jwt_token.split('.')[1]
                payload += '=' * (-len(payload) % 4) # Add padding if necessary
                decoded = base64.urlsafe_b64decode(payload).decode('utf-8')
                token_dict = json.loads(decoded)
                user_email = token_dict.get("email", "Unknown email")
                user_roles = token_dict.get("roles", "Unknown roles")
            except:
                user_email = "Unique user(decoding failed)"
                user_roles = "Unique user(decoding failed)"
        else:
            user_email = "Anonymous"
            user_roles = "Anonymous"

        yield {
                "job_id": job_id,
                "session_id": session_id,
                "instrument": instrument,
                "product_type": product_type,
                "job_status": job_status,
                "user_email": user_email,
                "user_roles": user_roles
            }

def main():
    '''
    Main function which will connect scan_directory func with db handler.
    '''
    scratch_dir = os.getenv("SCRATCH_DIR", "/data/dispatcher_scratch/")
    db_host = os.getenv("DB_HOST", "mysql") # k8 service name for the mysql pod
    db_user = os.getenv("DB_USER", "stats_bot")
    db_name = os.getenv("DB_NAME", "mmoda_usage")

    db_pass = os.getenv("DB_PASS")
    if not db_pass:
        pass_error = "DB_PASS environment variable is missing. Cannot connect to the database."
        logger.error(pass_error)
        raise ValueError(pass_error)

    logger.info(f"Starting oda-usage-monitor. Connecting to database at {db_host}...")
    db_handler = DatabaseHandler(host=db_host, user=db_user, password=db_pass, database=db_name)

    # Infinite loop to keep the sidecar running forever

    logger.info(f"Monitoring initialized. Target directory: {scratch_dir}")

    while True:
        try:
            jobs_processed = 0
            for job_data in scan_directory(scratch_dir):
                db_handler.upsert_job(job_data)
                jobs_processed += 1
            logger.info(f"Scan cycle completed. Jobs processed: {jobs_processed}")

        except Exception as e:
            logger.error(f"Critical error during scan cycle: {e}", exc_info=True)
        
        # Sleep for an hour before the next scan cycle
        time.sleep(3600)
        

if __name__ == "__main__":
    main()
    