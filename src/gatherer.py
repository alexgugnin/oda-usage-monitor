import os, glob, json, base64
from db_handler import DatabaseHandler


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
    paths_for_job_monitors = os.path.join(scratch_dir, "scratch_sid_*/job_monitor.json")

    #List of Tuples of two paths - one for the analysis parameters and one for the job monitor.
    #May be a problem if job monitor is creating after the analysis parameters?
    job_files = zip(sorted(glob.glob(paths_for_analysis_parameters)), sorted(glob.glob(paths_for_job_monitors)))
    
    for job_analysis_path, job_monitor_path in job_files:
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
    db_name = os.getenv("DB_NAME", "mmoda_metrics")

    db_pass = os.getenv("DB_PASS")
    if not db_pass:
        raise ValueError("DB_PASS environment variable is missing. Cannot connect to the database.")

    db_handler = DatabaseHandler(host=db_host, user=db_user, password=db_pass, database=db_name)

    # Infinite loop to keep the sidecar running forever
    import time

    print(f"Scanning {scratch_dir}...")
    while True:
        try:
            for job_data in scan_directory(scratch_dir):
                db_handler.upsert_job(job_data)
        except Exception as e:
            print(f"Error during scan cycle: {e}")
        
        # Sleep for an hour before the next scan cycle
        time.sleep(3600)
        

if __name__ == "__main__":
    main()
    