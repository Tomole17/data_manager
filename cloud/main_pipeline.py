# main_pipeline.py
import subprocess
import sys
import os

def run_script(script_path):
    print(f"Running: {script_path}...")
    
    # We create a copy of the current environment and force PYTHONIOENCODING to utf-8
    # This tells the sub-scripts to use UTF-8 regardless of the Windows terminal settings.
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    
    result = subprocess.run(
        [sys.executable, script_path], 
        capture_output=True, 
        text=True, 
        encoding='utf-8',
        env=env  # Passes the UTF-8 requirement to the sub-script
    )
    
    if result.returncode == 0:
        # We no longer encode/decode to ASCII. We print the raw UTF-8 string.
        print(f"SUCCESS: {script_path} finished.")
        if result.stdout:
            # We use .encode().decode() only to safely handle potential console mismatches
            # but we use 'utf-8' now, NOT 'ascii'.
            print(result.stdout.strip())
        return True
    else:
        print(f"ERROR in {script_path}:")
        print(result.stderr.strip())
        return False

def main():
    # Force the main script's output to UTF-8 to handle the Rocket emoji in the print below
    if sys.stdout.encoding != 'utf-8':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    print("🚀 Starting Daily Football Data Pipeline...")
    
    pipeline_steps = [
        "scripts/ingest_raw_data.py",
        "scripts/transform_to_silver.py",
        "scripts/generate_gold_ranking.py"
    ]
    
    for step in pipeline_steps:
        success = run_script(step)
        if not success:
            print("🛑 Pipeline halted due to error.")
            break
    else:
        print("🎉 Full Pipeline Executed Successfully!")

if __name__ == "__main__":
    main()